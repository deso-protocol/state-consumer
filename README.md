# State Consumer for DeSo Blockchain

The **State Consumer** is a Golang interface and framework designed to extract, decode, and process on-chain state from the DeSo (Decentralized Social) blockchain. The repository's purpose is to provide a flexible, datastore-agnostic mechanism that reads a binary state change file (produced by a DeSo node), decodes the state change entries, and then applies those changes (insert, update, or delete) via a configurable handler.

> **Note:** A working DeSo node must be configured to generate state change files. This file serves as the source of truth for the consumer, while the state handler (for example, a Postgres data handler) applies those changes into the target datastore.

---

## Key Concepts

### 1. DeSo Node
A **DeSo Node** is a running instance on the DeSo blockchain. It:
- **Syncs the blockchain:** Initializes by syncing from other nodes.
- **Processes on-chain transactions:** Continuously updates its state as new transactions are mined.
- **Generates State Change Files:** Writes a binary log of state changes (state change file) that the state consumer uses as input.
  
*Tip:* Make sure your node is configured to create and expose these state change files (usually via a shared directory or volume).

### 2. State Consumer Interface
The state consumer interface is a Go interface that encapsulates the following:
- **Reading the State Change File:** It can read binary state change files produced by a DeSo node.
- **Decoding State Change Entries:** It decodes the individual state change entries (which include operations such as insert, update, or delete).
- **Calling Handler Functions:** It provides a hook to call a handler function for processing those decoded entries.

This interface is purposely designed to be **datastore agnostic**, which means you can implement a handler to store the on-chain state in your database or any other system.

### 3. Data Handler Service
A **Data Handler Service** is a concrete implementation of the state consumer interface. For example, the [Postgres Data Handler](https://github.com/deso-protocol/postgres-data-handler) is one such implementation that:
- **Processes entries in batches:** For high performance and consistency.
- **Handles transaction management:** Using transaction savepoints, batching, and optional mempool syncing.
- **Applies state changes into a datastore:** In this case, the service performs insert, update, and delete operations on a PostgreSQL database.

Since the core consumer is datastore independent, you can create your own handler if you wish to persist the state in an alternative datastore (for example, a key–value store, another SQL database, or an in-memory cache).

### 4. StateChangeEntry Encoder

The **StateChangeEntry encoder** is a critical component of the syncing process. The type `StateChangeEntry` defines the structure of each state operation recorded by the DeSo node. Below is a detailed explanation of each property:

- **OperationType (StateSyncerOperationType):**
  - Specifies the type of operation (insert, update, or delete) that should be performed on the datastore.

- **KeyBytes ([]byte):**
  - Represents the key that identifies the record in the core Badger DB. This could relate to various entities like posts or profiles.

- **Encoder (DeSoEncoder):**
  - Holds the encoder instance responsible for serializing and deserializing the entity's data.
  - This encoder abstracts the logic needed to translate structured data into a binary format.

- **EncoderBytes ([]byte):**
  - Contains a raw byte representation of the encoder.
  - During operations such as hypersync, rather than re-encoding the data, the raw bytes are stored directly for improved performance.

- **AncestralRecord (DeSoEncoder):**
  - Stores the previous state (ancestral record) of the data that can be used to revert changes.
  - This is especially crucial for mempool transactions where state changes might need to be reverted upon block confirmation.

- **AncestralRecordBytes ([]byte):**
  - A raw byte representation of the ancestral record.
  - Used for efficiently restoring an earlier state without re-encoding.

- **EncoderType (EncoderType):**
  - Indicates which type of encoder is used. Different encoder types correspond to different on-chain data formats (e.g., posts, profiles, transactions).
  - This enables the consumer to properly decode the binary data.

- **FlushId (uuid.UUID):**
  - Uniquely identifies the flush batch that this state change belongs to.
  - It helps group multiple state changes that occur during the same flush operation.

- **BlockHeight (uint64):**
  - Denotes the block height at which the state change occurred.
  - Acts as a temporal marker to ensure the correct ordering and consistency of state updates.

- **Block (*MsgDeSoBlock):**
  - For UTXO-based operations or when block-related data is relevant, this field contains the block information associated with the state change.
  - It is only applicable for specific operation types.

- **IsReverted (bool):**
  - A flag indicating whether the state change has been reverted.
  - Particularly useful in mempool scenarios where previously applied entries might need to be undone.

By encapsulating all of these fields, the StateChangeEntry encoder provides a robust mechanism for serializing the complete on-chain state, ensuring that each update can be accurately applied or reversed during the syncing process.

---

## How It Works

1. **DeSo Node Setup:**  
   Your DeSo node must be configured to write a state change file (and optionally an index/state progress file) to a directory (e.g., `/state-changes`).

2. **Running the State Consumer:**  
   The state consumer reads from the state change file, decodes the on-chain state change entries, and then calls the respective handler functions. These functions then process each entry based on its operation type (insert/update/delete) and the specific encoder type (e.g., posts, profiles, likes).

3. **Implement/Customize the Data Handler:**
   - For a complete, working example of a data handler, please refer to the [Postgres Data Handler](https://github.com/deso-protocol/postgres-data-handler) repository.
   - To create your own data handler, implement the methods defined by the `StateSyncerDataHandler` interface as described in [`consumer/interfaces.go`](./consumer/interfaces.go).

4. **Extensibility:**  
   Because the state consumer interface is designed to be datastore agnostic, you have the flexibility to write handlers for any back-end storage or processing system without modifying the core consumer logic.

---

## Getting Started

### Prerequisites
- A working DeSo node configured to emit the state change file.
- Go (Golang) installed.
- Familiarity with building and running Docker containers if you plan to deploy across multiple services.

### Quick Setup Guide

1. **Clone this Repository:**
   ```bash
   git clone https://github.com/deso-protocol/state-consumer.git
   cd state-consumer
   ```

2. **Configure Environment Variables:**
   Ensure that your environment has the following variables (or equivalent configuration):
   - `STATE_CHANGE_DIR`: Path to the state change file directory.
   - `CONSUMER_PROGRESS_DIR`: Directory to store consumer progress files.
   - Additional settings for batching (e.g., `BATCH_BYTES`, `THREAD_LIMIT`) if needed.

3. **Implement/Customize the Data Handler:**
   - For a complete, working example of a data handler, please refer to the [Postgres Data Handler](https://github.com/deso-protocol/postgres-data-handler) repository.
   - To create your own data handler, implement the methods defined by the `StateSyncerDataHandler` interface as described in [`consumer/interfaces.go`](./consumer/interfaces.go).


### Deployment Recommendations

- **Containerization:**  
  When deploying in production, consider a multi-container setup:
  1. **DeSo Node Container:** Generates the state change file.
  2. **State Consumer Container:** Reads the file and applies changes via your custom data handler.
  3. **Target Data Store Container:** (e.g., Postgres, Elasticsearch, etc.) which stores the on-chain state.
  
- **Networking and Volumes:**  
  Make sure the state consumer container has access to the state change file directory (using shared volumes or network file shares).

- **Monitoring and Logging:**  
  Use proper logging (e.g., via glog) and monitoring tools to track syncing progress and detect errors.

---

## Contributing

Contributions, bug fixes, and feature requests are welcome. Please feel free to open an issue or submit a pull request.

## Have more questions?

DeepWiki (powered by Devin AI) provides up-to-date documentation you can talk to for this repo, click the button below to try it out.

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/deso-protocol/state-consumer)

