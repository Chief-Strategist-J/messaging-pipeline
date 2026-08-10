# Event Naming Conventions

1. **Lower Snake Case**: Event names must consist of lowercase alphanumeric characters and underscores only.
2. **Verb-Noun / Subject-Action**: Prefer descriptive action-oriented names (e.g., `page_view`, `purchase_completed`, `heartbeat`).
3. **No Redundant Prefixes**: Avoid redundant prefixing like `event_page_view`.
4. **Registration First**: Any new event must be registered in [registry.yaml](file:///home/btpl-lap-22/live/messaging-pipeline/shared/events/registry.yaml) before implementation.
