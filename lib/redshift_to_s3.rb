require "helper/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'slack-notifier'
require 'zlib'
require 'tempfile'
require "helper/table"
require "helper/column"
require "helper/slack_notifier"
require "pry-rails"

class RedshiftToS3
  class << self
    attr_accessor :source_uri, :source_schema, :source_table, :archive_date
  end

  attr_reader :source_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.archive_tables
    archive_tables = RedshiftToS3.new
    if archive_tables.tables.size == 0
      message = "[RS2S3]MISSING: Table(s) not found using the following parameters:\n[RS2S3]MISSING: source_schema: #{ENV["RS2S3_SOURCE_SCHEMA"]}\n[RS2S3]MISSING: source_table: #{ENV["RS2S3_SOURCE_TABLE"]}"
      SLACK_NOTIFIER.ping message
    end
    archive_tables.tables.each do |table|
      archive_tables.copy_table(table)
    end
  rescue => e
    SLACK_NOTIFIER.ping "[RS2S3]#{e.message.gsub("\r"," ").gsub("\n"," ")} | SCHEMA: #{RedshiftToS3.source_schema} | TABLE: #{RedshiftToS3.source_table} | DATE: #{RedshiftToS3.archive_date}"
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['RS2S3_SOURCE_URI'])
  end

  def self.source_schema
    @source_schema ||= ENV['RS2S3_SOURCE_SCHEMA']
  end

  def self.source_table
    @source_table ||= ENV['RS2S3_SOURCE_TABLE']
  end

  def self.archive_date
    @archive_date ||= Time.now.strftime("%F")
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: URI.decode(source_uri.password), dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def source_connection
    self.class.source_connection
  end

  def self.slack_on_success
    @slack_on_success ||= ENV['SLACK_ON_SUCCESS']
  end

  def tables
    table_command = <<-SQL
      SELECT t.*
      FROM information_schema.tables t
      WHERE t.table_schema = '#{RedshiftToS3.source_schema}' AND t.table_name = '#{RedshiftToS3.source_table}'
    SQL
    source_connection.exec(table_command).map do |table_attributes|
    table = Helper::Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    column_command = <<-SQL
      SELECT *
      FROM information_schema.columns
      WHERE table_schema = '#{RedshiftToS3.source_schema}' AND table_name='#{table.name}'
      ORDER BY ordinal_position
    SQL
    source_connection.exec(column_command)
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['RS2S3_S3_EXPORT_ID'], secret_access_key: ENV['RS2S3_S3_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['RS2S3_S3_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new("rd2s3")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1
    timestamp = Time.now.to_i

    begin
      copy_to_command = <<-SQL
          SELECT *
          FROM #{RedshiftToS3.source_schema}.#{table.name}
      SQL
      puts "starting to fetch table"
      records = source_connection.exec(copy_to_command)
      puts "table fetched."

      records.each_with_index do |row, index|
        formatted_row = row.values.map { |a| a.nil? ? "\\N" : a.gsub("|", "\\|") }.join("|")
        zip.write(formatted_row += "\n")
        if (zip.pos > chunksize)
          zip.finish
          tmpfile.rewind
          upload_table(table, tmpfile, chunk, timestamp)
          chunk += 1
          zip.close unless zip.closed?
          tmpfile.unlink
          tmpfile = Tempfile.new("rd2s3")
          zip = Zlib::GzipWriter.new(tmpfile)
        end
        puts "#{index} records done.." if (index % 100_000).zero?
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile, chunk, timestamp)
      if (RedshiftToS3.slack_on_success == 'true')
        message = "[RS2S3]SUCCESS: Archived #{RedshiftToS3.source_schema}/#{RedshiftToS3.source_schema}-#{table.target_table_name}-#{RedshiftToS3.archive_date} | Total Chunk(s): #{chunk} | SCHEMA: #{RedshiftToS3.source_schema} | TABLE: #{RedshiftToS3.source_table} | DATE: #{RedshiftToS3.archive_date}"
        SLACK_NOTIFIER.ping message
      end
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(table, buffer, chunk, timestamp)
    puts "uploading table"
    bucket.objects["#{RedshiftToS3.source_schema}/#{RedshiftToS3.source_schema}-#{table.target_table_name}-#{RedshiftToS3.archive_date}-#{timestamp}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)

    if (RedshiftToS3.slack_on_success == 'true')
      message = "[RS2S3]FINISH: Archived #{RedshiftToS3.source_schema}/#{RedshiftToS3.source_schema}-#{table.target_table_name}-#{RedshiftToS3.archive_date}-#{timestamp}.psv.gz.#{chunk}"
      SLACK_NOTIFIER.ping message
    end
  end
end
