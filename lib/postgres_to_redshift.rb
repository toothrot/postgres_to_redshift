require "helper/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require "helper/table"
require "helper/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri, :target_schema, :source_schema, :delete_option
  end

  attr_reader :source_connection, :target_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      #target_connection.exec("CREATE TABLE IF NOT EXISTS #{target_schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      update_tables.copy_table(table)

      update_tables.import_table(table)
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['P2RS_SOURCE_URI'])
  end

  def self.source_schema
    @source_schema ||= ENV['P2RS_SOURCE_SCHEMA']
  end

  def self.target_schema
    @target_schema ||= ENV['P2RS_TARGET_SCHEMA']
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['P2RS_TARGET_URI'])
  end

  def self.delete_option
    @delete_option ||= ENV["P2RS_DELETE_OPTION"]
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
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = '#{PostgresToRedshift.source_schema}' AND table_type in ('BASE TABLE') AND table_name NOT IN ('ar_internal_metadata','schema_migrations','audits') AND LEFT(table_name,1) != '_'").map do |table_attributes|
      table = Helper::Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema = '#{PostgresToRedshift.source_schema}' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['P2RS_S3_EXPORT_ID'], secret_access_key: ENV['P2RS_S3_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['P2RS_S3_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2rs")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1

    bucket.objects.with_prefix("#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz").delete_all

    begin
      puts "DOWNLOADING #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{PostgresToRedshift.source_schema}.#{table.name}) TO STDOUT WITH DELIMITER '|'"

      source_connection.copy_data(copy_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
          if (zip.pos > chunksize)
            zip.finish
            tmpfile.rewind
            upload_table(table, tmpfile, chunk)
            chunk += 1
            zip.close unless zip.closed?
            tmpfile.unlink
            tmpfile = Tempfile.new("psql2rs")
            zip = Zlib::GzipWriter.new(tmpfile)
          end
        end
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile, chunk)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(table, buffer, chunk)
    puts "UPLOADING #{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz.#{chunk}"

    bucket.objects["#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)

  end

  def import_table(table)

    puts "IMPORTING #{PostgresToRedshift.target_schema}.#{table.target_table_name}"

    if PostgresToRedshift.delete_option == 'drop'

      puts "DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("DROP TABLE IF EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}")

      puts "CREATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("CREATE TABLE #{PostgresToRedshift.target_schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      puts "COPY TABLE to #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("COPY #{PostgresToRedshift.target_schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['P2RS_S3_EXPORT_BUCKET']}/#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['P2RS_S3_EXPORT_ID']};aws_secret_access_key=#{ENV['P2RS_S3_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|' COMPUPDATE ON;")

    elsif PostgresToRedshift.delete_option == 'truncate'

      puts "CREATE TABLE IF NOT EXISTS #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("CREATE TABLE IF NOT EXISTS #{PostgresToRedshift.target_schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      puts "TRUNCATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("TRUNCATE TABLE #{PostgresToRedshift.target_schema}.#{table.target_table_name}")

      puts "COPY TABLE to #{PostgresToRedshift.target_schema}.#{table.target_table_name}"
      target_connection.exec("COPY #{PostgresToRedshift.target_schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['P2RS_S3_EXPORT_BUCKET']}/#{PostgresToRedshift.target_schema}/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['P2RS_S3_EXPORT_ID']};aws_secret_access_key=#{ENV['P2RS_S3_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|' COMPUPDATE ON;")

    else
      puts "missing delete_option"
    end
  end
end
