# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [2.1.2] - 2019-08-09
### Changed
 - Update to Avro 1.9.0 from 1.8.2
 - Remove patched Avro code
 
## [2.1.1] - 2017-08-07
### Changed
 - Fixed import of shaded guava class via Avro.
 
## [2.1.0] - 2017-07-24
### Added
 - Repo/license badges in README.
 
### Changed
 - Moved to GitHub.

## [2.0.0] - 2017-06-14
### Added
- Change log.

### Changed
- Update parent to OSS:1.2.0 to avoid platform JDK 1.8 issues.
- Update to Avro 1.8.2 from 1.8.1.
- Integrated latest changes from [AVRO-2003](https://github.com/apache/avro/pull/201) patch (latest commit `cd47f43a4124086aa0aeeb30d158cc40a9564e30`)
- Modified messaging to report multiple incompatibilities in each schema version.

## [1.0.0] - 2017-02-28
### Added
- Initial version.
