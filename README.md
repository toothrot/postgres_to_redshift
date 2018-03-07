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

## Contributing

1. Fork it ( https://github.com/kitchensurfing/postgres_to_redshift/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

UPDATES 2017-04-28
1. Remove any operational tables from moving over to AWS (table_name NOT IN ('ar_internal_metadata','schema_migrations') AND LEFT(table_name,1) != '_')
2. Add COMPUPDATE ON to enable automatic compression during COPY command
3. Automtically assign "sortkey distkey" to "id" column

UPDATES 2017-05-03
1. Modify to gem to load data from Audit to S3 only
2. New Class call postgres_to_s3


Difference between RS and S3
RS: export data from posgres without HEADER, load entire table as-is
S3: export data with HEADER, COPY command must ignore first row

RS: interim files in S3 is deleted from S3 bucket on the next data load
S3: unless an existing files already exist, files will never get deleted