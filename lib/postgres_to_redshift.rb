require 'postgres_to_redshift/version'
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require 'postgres_to_redshift/table'
require 'postgres_to_redshift/column'
require 'postgres_to_redshift/copy_import'

module PostgresToRedshift
  def self.update_tables
    tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")
      CopyImport.new(table: table, bucket: bucket, source_connection: source_connection, target_connection: target_connection, schema: schema).run
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
    end

    @source_connection
  end

  def self.target_connection
    @target_connection ||= PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
  end

  def self.schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def self.tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW')").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/

      if ENV['REDSHIFT_INCLUDE_TABLES'].present?
        next unless ENV['REDSHIFT_INCLUDE_TABLES'].split(',').include?(table.name)
      end
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def self.column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def self.s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def self.bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end
end
