job-trends-pipeline/
│
├── README.md                          
├── docs/
│   ├── SRS.md                         
│   ├── data-model.md                  
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
├── src/                               
│   ├── ingestion/
│   ├── transformation/
│   └── serving/
│
└── .github/
    └── ISSUE_TEMPLATE.md
