# Storing test results

Based on https://www.getdbt.com/blog/dbt-live-apac-tracking-dbt-test-success/

packages.yml
```yaml
packages:
  - git: "https://github.com/badal-io/dbt-store-test-results.git"
    revision: 0.1
```

dbt_project.yml
```yaml
on-run-end:
  - "{{ dbt_store_test_results.store_test_results(results) }}"
```
