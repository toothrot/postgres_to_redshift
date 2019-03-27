module PostgresToRedshift
  class Column
    CAST_TYPES_FOR_COPY = {
      'text' => 'CHARACTER VARYING(65535)',
      'json' => 'CHARACTER VARYING(65535)',
      'jsonb' => 'CHARACTER VARYING(65535)',
      'bytea' => 'CHARACTER VARYING(65535)',
      'money' => 'DECIMAL(19,2)',
      'oid' => 'CHARACTER VARYING(65535)',
      'ARRAY' => 'CHARACTER VARYING(65535)',
      'USER-DEFINED' => 'CHARACTER VARYING(65535)',
      'uuid' => 'CHARACTER VARYING(36)'
    }.freeze

    def initialize(attributes:)
      @attributes = attributes
    end

    def name
      attributes['column_name']
    end

    def name_for_copy
      if needs_type_cast?
        %[CAST("#{name}" AS #{data_type_for_copy}) AS #{name}]
      else
        %("#{name}")
      end
    end

    def data_type
      attributes['data_type']
    end

    def data_type_for_copy
      CAST_TYPES_FOR_COPY[data_type] || data_type
    end

    private

    attr_reader :attributes

    def needs_type_cast?
      data_type != data_type_for_copy
    end
  end
end
