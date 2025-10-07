## Directory Overview
This directory is intended for the development of a Go-based AMQP 0.9.1 server implementation as outlined in the plan.md file. Currently, it contains the project plan and this context file.

## Project Plan Summary
The project aims to create a Go package `github.com/maxpert/amqp-go` that implements an AMQP 0.9.1 server based on the official specification. It is broken down into several phases:

1.  **Project Setup and Core Protocol Foundation:** Initialize the module, define core data structures, implement binary frame handling, basic TCP server, and connection/channel management.
2.  **Exchange and Queue Management:** Implement core exchange types and queue operations.
3.  **Message Publishing and Consumption:** Implement basic publishing, consuming, and acknowledgment mechanisms.
4.  **Persistence and Reliability:** Integrate a storage backend and implement message durability.
5.  **Advanced Features and Security:** Add features like transactions, access control, and TLS/SSL support.
6.  **Testing and Refinement:** Comprehensive testing, benchmarking, and documentation.

## Key Files
- `plan.md`: Contains the detailed plan, phases, and questions for the AMQP server implementation.
- `CLAUDE.md`: This context file for Claude Code.

## Project Type
This is intended to be a **Go code project** implementing an AMQP 0.9.1 server library.

## Memories

- Never write bogus test cases that don't actually test the code
- Follow broadly followed Go community guidelines
- Always have performance considerations when using a particular approach or library
- Always commit changes on every major step, update the corresponding plan file before a commit
- Commit messages should be detailed and opensource standard
- Prefer TDD, if the test doesn't work it can't be a commit! 
- When in doubt ask questions, first look internet for what is most obvious answer and then ask question with recommendation
- Always format changes according to go standards before compiling.
- always update plan.md file after you have completed a step. No checkin should go without updated plan.md