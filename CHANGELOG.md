# Changelog

## [unreleased]

* Allow delayed schedule of tasks, use clearer name for database columns
* Add `get_many` helper to retrieve multiple tasks with a single DB call
* Add `add_many` helper to insert multiple tasks with a single transaction
* Create an index on the table, can now scale to millions of tasks

## 0.0.6 - 2024-02-13

* Adding a task now returns its UUID, previously nothing was returned

## 0.0.5 - 2023-11-06

* Upgraded dependencies, now this library requires Python 3.9 (previously was 3.8)


## 0.0.4 - 2023-11-06

* Change name of `check_expired_leases()` to make it a public method

## 0.0.3 - 2023-10-05

* Add function to delete old completed tasks

## 0.0.2 - 2023-05-15

* Improve types and formatting
* Task is now in its own column, metadata is kept apart
* Upgrades to the CI/CD pipeline

## 0.0.1 - 2023-05-10

* First release
