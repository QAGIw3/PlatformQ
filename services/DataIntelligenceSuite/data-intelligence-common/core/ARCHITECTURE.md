# Core Module Architecture

This document clarifies the purpose and relationships between core modules in the data-intelligence-common library.

## Module Overview

### `core/pipelines/` - Data Processing Pipelines
**Purpose**: Building and executing data transformation pipelines

- **Focus**: Data flow and transformation
- **Use Cases**: ETL/ELT, data processing workflows, stream processing
- **Key Classes**: 
  - `PipelineBuilder` - Fluent API for building pipelines
  - `Pipeline` - Represents a data processing pipeline
  - `PipelineStage` - Individual transformation step

**Example**:
```python
pipeline = (PipelineBuilder()
    .source("s3://bucket/data")
    .filter(lambda x: x["value"] > 100)
    .transform(lambda x: {"id": x["id"], "normalized": x["value"] / 1000})
    .sink("cassandra://table")
    .build())
```

### `core/orchestration/` - Service and Workflow Orchestration
**Purpose**: Coordinating services, managing distributed workflows, and handling complex business processes

- **Focus**: Service coordination and workflow management
- **Use Cases**: Microservice orchestration, distributed transactions, saga patterns
- **Key Classes**:
  - `WorkflowOrchestrator` - Business workflow management
  - `EventOrchestrator` - Event-driven orchestration
  - `DistributedOrchestrator` - Multi-service coordination
  - `PipelineOrchestrator` - Orchestrates pipeline execution (not building)

**Example**:
```python
workflow = WorkflowOrchestrator()
workflow.add_step("validate_order", validate_service.validate)
workflow.add_step("process_payment", payment_service.charge)
workflow.add_step("fulfill_order", fulfillment_service.ship)
await workflow.execute(order_data)
```

## Key Differences

| Aspect | Pipelines | Orchestration |
|--------|-----------|---------------|
| **Primary Focus** | Data transformation | Service coordination |
| **Granularity** | Data record level | Service/task level |
| **State Management** | Data lineage | Workflow state |
| **Error Handling** | Data quality issues | Service failures |
| **Scaling** | Data parallelism | Task distribution |
| **Typical Duration** | Minutes to hours | Seconds to days |

## When to Use Each

### Use `core/pipelines/` when:
- Building ETL/ELT workflows
- Processing streams of data
- Applying transformations to datasets
- Building data processing DAGs
- Focus is on data movement and transformation

### Use `core/orchestration/` when:
- Coordinating multiple microservices
- Implementing saga patterns
- Managing long-running business processes
- Handling distributed transactions
- Building event-driven architectures

## The Overlap: PipelineOrchestrator

The `PipelineOrchestrator` in the orchestration module is responsible for:
- **Scheduling** pipeline execution
- **Monitoring** pipeline progress
- **Handling** failures and retries at the pipeline level
- **Coordinating** resources for pipeline execution
- **Managing** dependencies between pipelines

It orchestrates the execution of pipelines built with `PipelineBuilder`, but doesn't handle the data transformation logic itself.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐         ┌──────────────────┐         │
│  │  Data Pipeline  │         │ Business Workflow │         │
│  │    (ETL/ELT)   │         │  (Order Process)  │         │
│  └────────┬────────┘         └────────┬─────────┘         │
│           │                            │                    │
├───────────┼────────────────────────────┼───────────────────┤
│           ▼                            ▼                    │
│  ┌─────────────────┐         ┌──────────────────┐         │
│  │ PipelineBuilder │         │WorkflowOrchestrator│        │
│  │                 │         │                   │         │
│  │ - source()      │         │ - add_step()      │         │
│  │ - transform()   │         │ - add_saga()      │         │
│  │ - sink()        │         │ - execute()       │         │
│  └─────────────────┘         └──────────────────┘         │
│                                                             │
│           ┌──────────────────────┐                         │
│           │ PipelineOrchestrator │                         │
│           │                      │                         │
│           │ - schedule()         │                         │
│           │ - monitor()          │                         │
│           │ - handle_failures()  │                         │
│           └──────────────────────┘                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Best Practices

1. **Don't mix concerns**: Keep data transformation logic in pipelines and service coordination in orchestration
2. **Use the right tool**: Don't use orchestration for simple data transformations
3. **Compose when needed**: Use PipelineOrchestrator to coordinate complex multi-pipeline workflows
4. **Keep it simple**: Start with pipelines for data processing, add orchestration only when you need service coordination 