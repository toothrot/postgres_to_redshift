# PostgresToRedshift

This gem copies data from postgres to redshift. It's especially useful to copy data from postgres to redshift in heroku.

[![Build Status](https://travis-ci.org/kitchensurfing/postgres_to_redshift.svg?branch=master)](https://travis-ci.org/kitchensurfing/postgres_to_redshift)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'postgres_to_redshift'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres_to_redshift

## Usage

Set your source and target databases, as well as your s3 intermediary.

*Note: Only set the `POSTGRES_TO_REDSHIFT_EXCLUDE_TABLE_PATTERN` if you want to exlude certain table(comma seperate for multiple value)*

*Note: All tables are by default included.*

*Note: set `DROP_TABLE_BEFORE_CREATE` to true will drop the table on target before creation(default value is false)*

*Note: `POSTGRES_TO_REDSHIFT_SOURCE_SCHEMA` is default to public*

```bash
export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='testing-data'
export POSTGRES_TO_REDSHIFT_SOURCE_SCHEMA='testing-data'
export S3_DATABASE_EXPORT_ID='yourid'
export S3_DATABASE_EXPORT_KEY='yourkey'
export S3_DATABASE_EXPORT_BUCKET='some-bucket-to-use'
export DROP_TABLE_BEFORE_CREATE = true
export POSTGRES_TO_REDSHIFT_EXCLUDE_TABLE_PATTERN = 'table-pattern-to-exclude1,table-pattern-to-exclude2'
export POSTGRES_TO_REDSHIFT_INCLUDE_TABLE_PATTERN = 'table-pattern-to-include1,table-pattern-to-include2'

postgres_to_redshift
```

## Contributing

1. Fork it ( https://github.com/kitchensurfing/postgres_to_redshift/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
