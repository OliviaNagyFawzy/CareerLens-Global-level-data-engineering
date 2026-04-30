job-trends-pipeline/
│
├── README.md                          ← Main project overview (most important file)
├── docs/
│   ├── SRS.md                         ← Software Requirements Specification
│   ├── data-model.md                  ← ERD explanation
│   └── diagrams/
│       ├── architecture-overview.png
│       ├── erd-job-trends.png
│       ├── dfd-level1.png
│       ├── activity-main-pipeline.png
│       ├── activity-dead-letter.png
│       ├── sequence-bronze-landing.png
│       ├── sequence-dbt-to-postgres.png
│       └── sequence-dead-letter-backfill.png
│
├── src/                               ← Your actual pipeline code (when ready)
│   ├── ingestion/
│   ├── transformation/
│   └── serving/
│
└── .github/
    └── ISSUE_TEMPLATE.md
