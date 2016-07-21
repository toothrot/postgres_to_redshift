require 'postgres_to_redshift/version'
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require 'postgres_to_redshift/table'
require 'postgres_to_redshift/column'

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

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
      @source_connection.exec('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;')
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
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def copy_table(table)
    tmpfile = Tempfile.new('psql2rs')
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1
    bucket.objects.with_prefix("export/#{table.target_table_name}.psv.gz").delete_all
    begin
      puts "Downloading #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

      source_connection.copy_data(copy_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
          if zip.pos > chunksize
            zip.finish
            tmpfile.rewind
            upload_table(table, tmpfile, chunk)
            chunk += 1
            zip.close unless zip.closed?
            tmpfile.unlink
            tmpfile = Tempfile.new('psql2rs')
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
    puts "Uploading #{table.target_table_name}.#{chunk}"
    bucket.objects["export/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    target_connection.exec("DROP TABLE IF EXISTS public.#{table.target_table_name}_updating")

    begin
      target_connection.exec('BEGIN;')

      target_connection.exec("ALTER TABLE public.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

      target_connection.exec("CREATE TABLE public.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      target_connection.exec("COPY public.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec('COMMIT;')

    rescue PG::InternalError => exception
      handle_pg_exception(table, exception)
    end
  end

  def handle_pg_exception(table, exception)
    target_connection.exec('ROLLBACK;')

    if exception.message.include?('stl_load_errors')
      puts exception.message
      puts "ERROR:  Last entry in Redshift's 'stl_load_errors' table:"
      print_last_redshift_loading_error

      if !ENV['WARN_ON_LOADING_ERROR'].nil? && ENV['WARN_ON_LOADING_ERROR'].casecmp('true') == 0
        puts "\nINFO:  Skipping '#{table.name}' and continuing on."
      else
        exit
      end
    else
      puts 'ERROR:  Unhandled PG error:'
      raise
    end
  end

  def print_last_redshift_loading_error
    error_row = target_connection.exec('SELECT * FROM pg_catalog.stl_load_errors ORDER BY starttime DESC LIMIT 1').first
    error_row.each do |k, v|
      puts "\t#{k}: #{v}"
    end
  end
end
