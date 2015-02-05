require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{table.target_table_name} (#{table.columns_for_create})")

      update_tables.copy_table(table)

      update_tables.import_table(table)
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
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW')").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      table.columns = column_definitions(table)
      table
    end
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}'")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def copy_table(table)
    buffer = StringIO.new
    zip = Zlib::GzipWriter.new(buffer)

    puts "Downloading #{table}"
    copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

    source_connection.copy_data(copy_command) do
      while row = source_connection.get_copy_data
        zip.write(row)
      end
    end
    zip.finish
    buffer.rewind
    upload_table(table, buffer)
  end

  def upload_table(table, buffer)
    puts "Uploading #{table.target_table_name}"
    bucket.objects["export/#{table.target_table_name}.psv.gz"].delete
    bucket.objects["export/#{table.target_table_name}.psv.gz"].write(buffer, acl: :authenticated_read)
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    target_connection.exec("DROP TABLE IF EXISTS public.#{table.target_table_name}_updating")

    target_connection.exec("BEGIN;")

    target_connection.exec("ALTER TABLE public.#{table.target_table_name} RENAME TO #{table.target_table_name}_updating")

    target_connection.exec("CREATE TABLE public.#{table.target_table_name} (#{table.columns_for_create})")

    target_connection.exec("COPY public.#{table.target_table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("COMMIT;")
  end
end
