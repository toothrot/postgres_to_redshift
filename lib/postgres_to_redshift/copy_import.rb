module PostgresToRedshift
  class CopyImport
    KILOBYTE = 1024
    MEGABYTE = KILOBYTE * 1024
    GIGABYTE = MEGABYTE * 1024
    CHUNK_SIZE = 3 * GIGABYTE
    BEGINNING_OF_TIME = Time.at(0).utc

    def initialize(table:, bucket:, source_connection:, target_connection:, schema:, incremental_from: BEGINNING_OF_TIME, incremental_to:)
      @table = table
      @bucket = bucket
      @source_connection = source_connection
      @target_connection = target_connection
      @schema = schema
      @incremental_from = incremental_from
      @incremental_to = incremental_to
    end

    def run
      copy_table
      import_table
    end

    private

    def select_sql
      select_sql = "SELECT #{table.columns_for_copy} FROM #{table.name}"
      select_sql += " WHERE #{incremental_column} BETWEEN '#{incremental_from.iso8601}' AND '#{incremental_to.iso8601}'" if incremental_column
      select_sql
    end

    def incremental_column
      @incremental_column ||= %w[updated_at created_at].detect { |column_name| table.column_names.include?(column_name) }
    end

    def new_tmpfile
      tmpfile = StringIO.new
      tmpfile.set_encoding('utf-8')
      tmpfile.binmode
      tmpfile
    end

    def start_chunk
      tmpfile = new_tmpfile
      zip = Zlib::GzipWriter.new(tmpfile)
      [tmpfile, zip]
    end

    def close_resources(zip:)
      zip.close unless zip.closed?
    end

    def finish_chunk(tmpfile:, zip:, chunk:)
      zip.finish
      tmpfile.rewind
      upload_table(tmpfile, chunk)
      close_resources(zip: zip)
    end

    def copy_table
      tmpfile, zip = start_chunk
      chunk = 1
      bucket.objects.with_prefix("export/#{table.target_table_name}.psv.gz").delete_all
      begin
        puts "Downloading #{table} changes between #{incremental_from} and #{incremental_to} at #{Time.now.utc}"
        copy_command = "COPY (#{select_sql}) TO STDOUT WITH DELIMITER '|'"

        source_connection.copy_data(copy_command) do
          while (row = source_connection.get_copy_data)
            zip.write(row)
            next unless zip.pos > CHUNK_SIZE

            finish_chunk(tmpfile: tmpfile, zip: zip, chunk: chunk)
            chunk += 1
            tmpfile, zip = start_chunk
          end
        end
        finish_chunk(tmpfile: tmpfile, zip: zip, chunk: chunk)
        source_connection.reset
      ensure
        close_resources(zip: zip)
      end
    end

    def upload_table(buffer, chunk)
      puts "Uploading #{table.target_table_name}.#{chunk}"
      bucket.objects["export/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer)
    end

    def import_table
      args = { table: table, target_connection: target_connection, schema: schema }
      import = incremental? ? IncrementalImport.new(**args) : FullImport.new(**args)
      import.run
    end

    def incremental?
      incremental_from != BEGINNING_OF_TIME
    end

    attr_reader :table, :bucket, :source_connection, :target_connection, :schema, :incremental_from, :incremental_to
  end
end
