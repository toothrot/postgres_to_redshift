require "helper/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require "helper/table"
require "helper/column"

class PostgresToS3
  class << self
    attr_accessor :source_uri, :source_schema, :source_table, :service_name, :archive_date
  end

  attr_reader :source_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.archive_tables
    archive_tables = PostgresToS3.new

    archive_tables.tables.each do |table|
      archive_tables.copy_table(table)
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['P2S3_SOURCE_URI'])
  end

  def self.source_schema
    @source_schema ||= ENV['P2S3_SOURCE_SCHEMA']
  end

  def self.source_table
    @source_table ||= ENV['P2S3_SOURCE_TABLE']
  end

  def self.service_name
    @service_name ||= ENV['P2S3_SERVICE_NAME']
  end

  def self.archive_date
    @archive_date ||= ENV['P2S3_ARCHIVE_DATE']
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def source_connection
    self.class.source_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = '#{PostgresToS3.source_schema}' AND table_name = '#{PostgresToS3.source_table}'").map do |table_attributes|
      table = Helper::Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema = '#{PostgresToS3.source_schema}' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['P2S3_S3_EXPORT_ID'], secret_access_key: ENV['P2S3_S3_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['P2S3_S3_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new("psql2s3")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1

    begin
      puts "DOWNLOADING #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{PostgresToS3.source_schema}.#{table.name} WHERE lower(service_name) = lower('#{PostgresToS3.service_name}') AND created_at::date = '#{PostgresToS3.archive_date}') TO STDOUT WITH DELIMITER '|'"

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
            tmpfile = Tempfile.new("psql2s3")
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
    timestamp = Time.now.to_i

    puts "UPLOADING #{PostgresToS3.service_name}/#{PostgresToS3.service_name}-#{PostgresToS3.archive_date}-#{table.target_table_name}-#{timestamp}.psv.gz.#{chunk}"

    bucket.objects["#{PostgresToS3.service_name}/#{PostgresToS3.service_name}-#{PostgresToS3.archive_date}-#{table.target_table_name}-#{timestamp}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)

  end
end
