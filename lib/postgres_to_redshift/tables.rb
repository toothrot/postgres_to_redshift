module PostgresToRedshift
  class Tables
    def initialize(source_connection:, target_connection:)
      @source_connection = source_connection
      @target_connection = target_connection
    end

    def all
      source_tables = self.class.fetch_tables(connection: source_connection)
      target_tables = self.class.fetch_tables(connection: target_connection)
      source_tables.reject { |source_table| target_tables.include?(source_table) }.map(&:dirty!)
      source_tables
    end

    class << self
      def column_definitions(connection:, table_name:)
        connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table_name}' ORDER BY ordinal_position")
      end

      def redshift_include_tables
        @redshift_include_tables ||= ENV['REDSHIFT_INCLUDE_TABLES'].split(',')
      end

      def tables_sql
        sql = "SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE', 'VIEW') AND table_name !~* '^pg_.*'"
        if ENV['REDSHIFT_INCLUDE_TABLES']
          table_names = "'" + redshift_include_tables.join("', '") + "'"
          sql += " AND table_name IN (#{table_names})"
        end
        sql += ' ORDER BY table_name'
        sql
      end

      def fetch_tables(connection:)
        connection.exec(tables_sql).map do |table_attributes|
          Table.new(attributes: table_attributes).tap do |table|
            table.columns = column_definitions(connection: connection, table_name: table.name)
          end
        end
      end
    end

    attr_reader :source_connection, :target_connection
  end
end
