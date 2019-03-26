module PostgresToRedshift
  class CopyImport
    KILOBYTE = 1024
    MEGABYTE = KILOBYTE * 1024
    GIGABYTE = MEGABYTE * 1024
    CHUNK_SIZE = 5 * GIGABYTE

    def initialize(table:, bucket:, source_connection:, target_connection:, schema:)
      @table = table
      @bucket = bucket
      @source_connection = source_connection
      @target_connection = target_connection
      @schema = schema
    end

    def run
      copy_table
      import_table
    end

    private

    def new_tmpfile
      tmpfile = Tempfile.new('psql2rs', encoding: 'utf-8')
      tmpfile.binmode
      tmpfile
    end

    def start_chunk
      tmpfile = new_tmpfile
      zip = Zlib::GzipWriter.new(tmpfile)
      [tmpfile, zip]
    end

    def close_resources(tmpfile:, zip:)
      zip.close unless zip.closed?
      tmpfile.unlink
    end

    def finish_chunk(tmpfile:, zip:)
      zip.finish
      tmpfile.rewind
      upload_table(tmpfile, chunk)
      close_resources(tmpfile: tmpfile, zip: zip)
    end

    def copy_table
      tmpfile, zip = start_chunk
      chunk = 1
      bucket.objects.with_prefix("export/#{table.target_table_name}.psv.gz").delete_all
      begin
        puts "Downloading #{table}"
        copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name}) TO STDOUT WITH DELIMITER '|'"

        source_connection.copy_data(copy_command) do
          while (row = source_connection.get_copy_data)
            zip.write(row)
            next unless zip.pos > CHUNK_SIZE

            chunk += 1
            finish_chunk(tmpfile: tmpfile, zip: zip)
            tmpfile, zip = start_chunk
          end
        end
        finish_chunk(tmpfile: tmpfile, zip: zip)
        source_connection.reset
      ensure
        close_resources(tmpfile: tmpfile, zip: zip)
      end
    end

    def upload_table(buffer, chunk)
      puts "Uploading #{table.target_table_name}.#{chunk}"
      bucket.objects["export/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)
    end

    def import_table
      puts "Importing #{table.target_table_name}"

      target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")

      target_connection.exec('BEGIN;')

      target_connection.exec("ALTER TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

      target_connection.exec("CREATE TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      target_connection.exec("COPY #{schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec('COMMIT;')
    end

    attr_reader :table, :bucket, :source_connection, :target_connection, :schema
  end
end
