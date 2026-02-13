# Controller

The Controller is a Linera application that manages the orchestration of distributed
services across multiple worker nodes. It provides a centralized registry for workers and
services, enabling dynamic assignment and coordination.

## Concepts

### Workers

A **Worker** is a node that can run services. Each worker:
- Runs on its own chain
- Registers itself with the controller, declaring its capabilities
- Receives commands from the controller to start/stop services
- Can follow additional chains as needed

### Services

A **Service** is a task or application component that runs on one or more workers. Services are identified by a `ServiceId` (a blob hash of their description). A service can be assigned to multiple workers for redundancy or load distribution.

### Controller Chain

The **Controller Chain** is the chain where the controller application was originally deployed. It serves as the central authority that:
- Maintains the registry of all workers
- Tracks which services run on which workers
- Processes admin commands to update service assignments

### Admin

**Admins** are authorized account owners who can execute controller commands to manage services. If no admin list is set, operations are permissive but remote commands are rejected for safety.

## Architecture

Workers register with the controller and receive Start/Stop messages for services.

```mermaid
graph TB
    subgraph "Controller Chain"
        CC[Controller Contract]
        WR[(Workers Registry)]
        SR[(Services Registry)]
        CC --> WR
        CC --> SR
    end
    subgraph "Worker Chain A"
        WA[Worker A]
        LSA[(Local Services)]
        WA --> LSA
    end
    subgraph "Worker Chain B"
        WB[Worker B]
        LSB[(Local Services)]
        WB --> LSB
    end
    subgraph "Worker Chain C"
        WC[Worker C]
        LSC[(Local Services)]
        WC --> LSC
    end
    WA -->|RegisterWorker| CC
    WB -->|RegisterWorker| CC
    WC -->|RegisterWorker| CC
    CC -->|Start/Stop| WA
    CC -->|Start/Stop| WB
    CC -->|Start/Stop| WC
```

## Message Flow

### Worker Registration

When a worker wants to join the network:

```mermaid
sequenceDiagram
    participant W as Worker Chain
    participant C as Controller Chain
    W->>W: prepare_worker_command_locally<br/>(set local_worker)
    W->>C: Message::ExecuteWorkerCommand<br/>{RegisterWorker}
    C->>C: execute_worker_command_locally<br/>(add to workers registry)
```

### Service Assignment

When an admin assigns a service to workers:

```mermaid
sequenceDiagram
    participant A as Admin
    participant C as Controller Chain
    participant W1 as Worker 1
    participant W2 as Worker 2
    A->>C: ExecuteControllerCommand<br/>{UpdateService}
    C->>C: Update services registry
    C->>W1: Start{service_id,<br/>owners_to_remove: {},<br/>start_height: None}
    C->>W2: Start{service_id,<br/>owners_to_remove: {},<br/>start_height: None}
    W1->>W1: Add to local_services
    W2->>W2: Add to local_services
```

### Service Handoff (Reassignment)

When a service is moved from one worker to another, all three chain types
participate in a two-phase handoff protocol. The new worker's owners are added
to the service chain first, then the old worker's owners are removed, ensuring
there is no gap in ownership.

```mermaid
sequenceDiagram
    participant A as Admin
    participant C as Controller Chain
    participant WA as Old Worker Chain
    participant S as Service Chain
    participant WB as New Worker Chain

    A->>C: ExecuteControllerCommand<br/>{UpdateService{service, [WB]}}
    C->>C: Record pending handoff
    C->>WA: Stop{service, new_owners: [WB]}

    note over WA,S: Phase 1: Add new owners
    WA->>S: AddOwners{service, [WB]}
    S->>S: Add WB as chain owner
    S->>WA: OwnersAdded{service, block_height}

    WA->>WA: Remove from local_services
    WA->>C: HandoffStarted{service, block_height}

    C->>C: Resolve pending handoff
    C->>WB: Start{service,<br/>owners_to_remove: [WA],<br/>start_height: block_height}
    WB->>WB: Add to local_pending_services

    note over WB,S: Phase 2: Remove old owners
    WB->>WB: StartLocalService at start_height
    WB->>S: RemoveOwners{[WA]}
    S->>S: Remove WA as chain owner
    WB->>WB: Move to local_services
```

### Service Removal

When a service is removed, Stop messages are sent to all workers. Each worker
initiates ownership cleanup through the service chain before removing the
service locally, following the same handoff protocol but with empty new owners.

```mermaid
sequenceDiagram
    participant A as Admin
    participant C as Controller Chain
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant S as Service Chain
    A->>C: ExecuteControllerCommand<br/>{RemoveService}
    C->>C: Remove from services registry
    C->>W1: Stop{service_id, new_owners: {}}
    C->>W2: Stop{service_id, new_owners: {}}

    W1->>S: AddOwners{service_id, {}}
    S->>W1: OwnersAdded{service_id, block_height}
    W1->>W1: Remove from local_services
    W1->>C: HandoffStarted{service_id, block_height}

    W2->>S: AddOwners{service_id, {}}
    S->>W2: OwnersAdded{service_id, block_height}
    W2->>W2: Remove from local_services
    W2->>C: HandoffStarted{service_id, block_height}
```

## Operations

### Worker Commands

| Command | Description |
|---------|-------------|
| `RegisterWorker { capabilities }` | Register a worker with its capabilities |
| `DeregisterWorker` | Remove the worker from the network |

### Controller Commands (Admin Only)

| Command | Description |
|---------|-------------|
| `SetAdmins { admins }` | Set the list of authorized admin accounts |
| `RemoveWorker { worker_id }` | Force-remove a worker and clean up its assignments |
| `UpdateService { service_id, workers }` | Assign a service to specific workers |
| `RemoveService { service_id }` | Remove a service from all workers |
| `UpdateAllServices { services }` | Bulk update all service assignments |

## State

### Controller Chain State

- `admins`: Set of authorized admin accounts
- `workers`: Map of ChainId to Worker (all registered workers)
- `services`: Map of ServiceId to Set of ChainIds (service assignments)

### Worker Chain State

- `local_worker`: This worker's registration info
- `local_services`: Set of services running on this worker
- `local_chains`: Additional chains this worker is following

## Messages

Messages are sent between chains to coordinate state:

| Message | Direction | Purpose |
|---------|-----------|---------|
| `ExecuteWorkerCommand` | Worker -> Controller | Register/deregister worker |
| `ExecuteControllerCommand` | Any -> Controller | Admin commands |
| `Reset` | Controller -> Worker | Clear worker state |
| `Start { service_id, owners_to_remove, start_height }` | Controller -> Worker | Start a service, optionally with handoff info |
| `Stop { service_id, new_owners }` | Controller -> Worker | Stop a service, initiating ownership handoff via the service chain |
| `FollowChain { chain_id }` | Controller -> Worker | Follow a chain |
| `ForgetChain { chain_id }` | Controller -> Worker | Stop following a chain |
