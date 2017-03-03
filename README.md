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

```bash
export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://username:password@host:port/database-name'
export S3_DATABASE_EXPORT_ID='yourid'
export S3_DATABASE_EXPORT_KEY='yourkey'
export S3_DATABASE_EXPORT_BUCKET='some-bucket-to-use'
export POSTGRES_TO_REDSHIFT_SOURCE_SCHEMA='schema_name'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='schema_name'  #make sure target_schema exist in target DB
export POSTGRES_TO_REDSHIFT_DELETE_OPTION='truncate|drop'	#this define whether the destination tables should be truncated or drop

postgres_to_redshift
```

## Contributing

1. Fork it ( https://github.com/kitchensurfing/postgres_to_redshift/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
