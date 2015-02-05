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
    update_tables.create_new_tables

    # FIXME: BIG WARNING HERE: the order of tables and views is important. We want the views to overwrite the tables. We should make it so the order doesn't matter later.
    update_tables.copy_tables
    update_tables.copy_views
    update_tables.import_tables
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
    self.class.source_connection
  end

  def views
    source_connection.exec("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'VIEW'").map { |row| row["table_name"] } - ["pg_stat_statements"]
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'").map { |row| Table.new(attributes: row) }
  end

  def table_columns(table_name)
    source_connection.exec("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table_name}'").map do |row| 
      data_type = row["data_type"]
      data_type.gsub!(/text/, 'character varying(max)')
      data_type.gsub!(/json/, 'character varying(max)')
      data_type.gsub!(/bytea/, 'character varying(max)')
      data_type.gsub!(/money/, 'character varying(max)')

      if row["character_maximum_length"].to_s.length > 0
        %Q|"#{row["column_name"]}" #{data_type}(#{row["character_maximum_length"]})|
      else
        %Q|"#{row["column_name"]}" #{data_type}|
      end
    end.join(", ")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def create_new_tables
    tables.each do |table|
      target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{table.name} (#{table_columns(table.name)})")
    end
  end

  def copy_table(source_table, target_table, is_view = false)
    buffer = ""
    puts "Downloading #{source_table}"
    copy_command = 
      if is_view
        "COPY (SELECT * FROM #{source_table}) TO STDOUT WITH DELIMITER '|'"
      else
        "COPY #{source_table} TO STDOUT WITH DELIMITER '|'"
      end

    source_connection.copy_data(copy_command) do
      while row = source_connection.get_copy_data
        buffer << row
      end
    end
    upload_table(target_table, buffer)
  end

  def upload_table(target_table, buffer)
    puts "Uploading #{target_table}"
    bucket.objects["export/#{target_table}.psv"].delete
    bucket.objects["export/#{target_table}.psv"].write(buffer, acl: :authenticated_read)
  end

  def import_table(target_table)
    puts "Importing #{target_table}"
    target_connection.exec("DROP TABLE IF EXISTS public.#{target_table}_updating")

    target_connection.exec("BEGIN;")

    target_connection.exec("ALTER TABLE public.#{target_table} RENAME TO #{target_table}_updating")

    target_connection.exec("CREATE TABLE public.#{target_table} (#{table_columns(target_table)})")

    target_connection.exec("COPY public.#{target_table} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{target_table}.psv' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

    target_connection.exec("COMMIT;")
  end

  def copy_tables
    tables.each do |table|
      copy_table(table.name, table.name)
    end
  end

  def copy_views
    views.each do |view|
      table = view.gsub(/_view/, '')
      copy_table(view, table, true)
    end
  end

  # FIXME: This relies on views being uploaded after tables.
  def import_tables
    tables.each do |table|
      import_table(table.name)
    end
  end
end
