# Changelog

## [0.3.0] - 2024-08-16
### Added
* `tests` folder and corresponding test to test the macro generating the spark ddl

### Changed
* Refactored spark types into it's own enum with a string representation
* Fixed some edge cases with List and inner strings, i.e. `Vec<String>` (fixing largeutf-8 conversion. Inner object was not converted)
