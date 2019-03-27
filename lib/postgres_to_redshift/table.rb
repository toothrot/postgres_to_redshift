module PostgresToRedshift
  class Table
    attr_reader :columns, :attributes

    def initialize(attributes:, columns: [])
      @attributes = attributes
      self.columns = columns
    end

    def name
      attributes['table_name']
    end
    alias to_s name

    def target_table_name
      name.gsub(/_view$/, '')
    end

    def columns=(column_definitions = [])
      @columns = column_definitions.map do |column_definition|
        Column.new(attributes: column_definition)
      end
    end

    def columns_for_create
      columns.map do |column|
        %("#{column.name}" #{column.data_type_for_copy})
      end.join(', ')
    end

    def columns_for_copy
      columns.map(&:name_for_copy).join(', ')
    end

    def column_names
      @column_names ||= columns.map(&:name)
    end

    def view?
      attributes['table_type'] == 'VIEW'
    end
  end
end
