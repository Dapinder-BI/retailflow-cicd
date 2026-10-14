# RetailFlow architecture

The diagram below distinguishes **data processing** from **CI/CD deployment**. All three logical environments share a single Databricks Free Edition workspace.

```mermaid
flowchart TB
  subgraph G["GitHub · version control and automation"]
    F["Feature branch"] --> PR["Pull request"]
    PR --> CI["GitHub Actions CI<br/>YAML + Python syntax"]
    CI --> M["Merge to main"]
    M --> CD["GitHub Actions CD<br/>Validate + deploy PROD bundle"]
  end

  subgraph DB["Databricks Free Edition · one workspace"]
    subgraph DEV["DEV · retailflow_dev"]
      DBR["Bronze<br/>dev_customers · dev_orders"] --> DS["Silver<br/>customers_clean · orders_clean"] --> DG["Gold<br/>customer_sales_summary"]
    end
    subgraph TEST["TEST · retailflow_test"]
      TBR["Bronze<br/>test_customers · test_orders"] --> TS["Silver<br/>customers_clean · orders_clean"] --> TG["Gold<br/>customer_sales_summary"]
    end
    subgraph PROD["PROD · retailflow_prod"]
      PBR["Bronze<br/>prod_customers · prod_orders"] --> PS["Silver<br/>customers_clean · orders_clean"] --> PG["Gold<br/>customer_sales_summary"]
    end
  end

  CD -. "bundle deployment (not pipeline execution)" .-> PROD
```

**Important distinctions:** CI checks syntax and YAML structure, but does not currently run PySpark unit tests. CD deploys to PROD; it does not currently promote sequentially through DEV and TEST. Bronze source tables already exist and are not created by the bundle. Each target has its own catalog, but all targets use the same Databricks workspace.
