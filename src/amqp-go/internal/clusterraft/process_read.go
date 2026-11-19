package clusterraft

// ProcessReadAllowed is checked inside the ordinary Read query (Group lock
// held). Peer activation closes this one-way latch before exposing transport.
func (g *Group) ProcessReadAllowed() bool {
	return g.cfg.ProcessDurability && g.store.node.writer.durability.processAllowed()
}
