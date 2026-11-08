package storage

import (
	"os"
	"sync/atomic"

	"github.com/maxpert/amqp-go/interfaces"
)

// syncpolicy.go — the crash-model sync policy (barrier-everywhere, 2026-11-07).
//
// THE BUG THIS FILE FIXES. Under crash_model: barrier the WAL confirm sites
// already issued fcntl(F_BARRIERFSYNC) (an fsync plus an ORDERING barrier, no
// drive write-cache flush) instead of fcntl(F_FULLFSYNC). But every OTHER
// steady-state durability site — the roll's directory sync, the WAL-file
// creation directory sync, the reclaim directory sync, the compaction fsync and
// directory sync, the segment seal/temp/dir syncs and the metadata atomic
// write — still issued a FULL flush. On APFS an F_FULLFSYNC drains the WHOLE
// drive write cache, so a single one of those issued while traffic is in flight
// stalls every concurrent barrier behind it (measured: confirm p95 270–297 ms
// during the W8 retired-file flush). With the fence only at the two confirm
// sites, the roll/creation/reclaim dir syncs alone fire 2–4.5×/s.
//
// THE RULE. In barrier mode (darwin) EVERY sync that can fire while the broker
// is serving traffic uses the fence; F_FULLFSYNC survives only at the one-shot
// lifecycle points (clean close, boot) and on the I/O-error repair path, which
// are off the traffic path by nature. In force_flush mode every site keeps the
// full drive-cache flush, byte-identical to before.
//
// See .notes/specs/task-barrier-everywhere.md §1 for the per-site table.

// syncPolicy is the crash-model-aware primitive selector for one durability
// site. It is a VALUE (not a package global) ON PURPOSE: several storages in
// one test binary must be able to differ. Construct it from the configured
// ConfirmSync with newSyncPolicy.
type syncPolicy struct {
	// fence is true iff the barrier confirm primitive is a genuine
	// ORDERING-ONLY fence on this platform (darwin F_BARRIERFSYNC). Off darwin
	// ConfirmFenceOrdersOnly is false, so the policy is always "full" there and
	// crash_model is inert (the Q5 Linux inertness).
	fence bool
	// site labels the full-flush counter (§ observability) when this policy's
	// full arm runs. Empty means "unspecified".
	site string
}

// newSyncPolicy builds the policy for a configured confirm mode.
func newSyncPolicy(confirm walConfirmSync) syncPolicy {
	return syncPolicy{fence: confirm == walConfirmBarrier && ConfirmFenceOrdersOnly}
}

// confirmSyncFromCrashModel maps the user-facing crash_model string to the
// internal confirm mode. An empty string is the barrier default, mirroring
// WALConfigFromEngine.
func confirmSyncFromCrashModel(model string) walConfirmSync {
	if model == interfaces.CrashModelForceFlush {
		return walConfirmForceFlush
	}
	return walConfirmBarrier
}

// withSite returns a copy of the policy carrying a site label for the
// full-flush counter.
func (p syncPolicy) withSite(site string) syncPolicy {
	p.site = site
	return p
}

// file applies the policy to a file-durability barrier.
func (p syncPolicy) file(f *os.File) error {
	if p.fence {
		return barrierSyncFileSite(f, p.site)
	}
	return fdatasyncFileSite(f, p.site)
}

// dir applies the policy to a directory-entry-durability barrier.
func (p syncPolicy) dir(path string) error {
	if p.fence {
		return syncDirFence(path)
	}
	return syncDir(path)
}

// --- full-flush observability (the barrier-arm gate) -----------------------

// Site labels for amqp_wal_full_flush_total{site}. They mirror the §1 table.
const (
	siteGroupCommit    = "group_commit"
	siteTxConfirm      = "tx_confirm"
	siteTxDir          = "tx_dir"
	siteRollDir        = "roll_dir"
	sitePreopenFlush   = "preopen_flush"
	siteReclaimDir     = "reclaim_dir"
	siteCompactionFile = "compaction_file"
	siteCompactionDir  = "compaction_dir"
	siteSegmentSeal    = "segment_seal"
	siteSegmentTemp    = "segment_temp"
	siteSegmentDir     = "segment_dir"
	siteSegmentClose   = "segment_close"
	siteSegmentMkdir   = "segment_mkdir"
	siteMetadataFile   = "metadata_file"
	siteMetadataDir    = "metadata_dir"
	siteRepair         = "repair"
	siteBootTail       = "boot_tail"
	siteBootMkdir      = "boot_mkdir"
	siteClose          = "close"
	siteDirFull        = "dir_full"
	siteFenceFallback  = "fence_fallback"
)

// walFullFlushSites is the list of site labels the metrics layer
// pre-initializes to zero, so amqp_wal_full_flush_total{site=...} is present
// (and therefore readable as 0) even on a pristine barrier-mode boot before any
// full flush has been issued.
var walFullFlushSites = []string{
	siteGroupCommit, siteTxConfirm, siteTxDir, siteRollDir, sitePreopenFlush,
	siteReclaimDir, siteCompactionFile, siteCompactionDir, siteSegmentSeal,
	siteSegmentTemp, siteSegmentDir, siteSegmentClose, siteSegmentMkdir,
	siteMetadataFile, siteMetadataDir, siteRepair, siteBootTail, siteBootMkdir,
	siteClose, siteDirFull, siteFenceFallback,
}

// WALFullFlushSites returns the known full-flush site labels. The metrics layer
// uses it to pre-initialize the labelled counter to zero.
func WALFullFlushSites() []string { return append([]string(nil), walFullFlushSites...) }

// walFullFlushTotal counts every darwin F_FULLFSYNC this process has issued,
// across every site. It is THE gate input for the barrier arm: it must stay 0
// for the whole measured window (the only full flushes allowed in barrier mode
// fire off the traffic path, at boot and at close).
//
// The count lives at the single choke point that issues the syscall
// (fullFsyncFD on darwin), so no site can silently escape it.
var walFullFlushTotal atomic.Uint64

// WALFullFlushTotal returns the number of F_FULLFSYNCs issued since process
// start. On non-darwin platforms it is always 0 (there is no F_FULLFSYNC).
func WALFullFlushTotal() uint64 { return walFullFlushTotal.Load() }

// walFullFlushSink is the production metrics sink for the labelled counter. It
// is nil until the server binds it; nil means "no metrics collector", which is
// the state of any storage built without a server.
var walFullFlushSink atomic.Pointer[func(site string)]

// SetWALFullFlushSink binds the sink the full-flush counter feeds. Called once
// by the server builder when the metrics collector implements the sink method.
// It is process-global because metrics are process-global.
func SetWALFullFlushSink(fn func(site string)) {
	if fn == nil {
		walFullFlushSink.Store(nil)
		return
	}
	walFullFlushSink.Store(&fn)
}

// recordFullFlush counts one issued F_FULLFSYNC. It is called only from the
// darwin full-flush choke point (fullFsyncFD), on the successful path.
func recordFullFlush(site string) {
	walFullFlushTotal.Add(1)
	if p := walFullFlushSink.Load(); p != nil {
		(*p)(site)
	}
}
