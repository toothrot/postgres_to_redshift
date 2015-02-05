require 'spec_helper'

RSpec.describe PostgresToRedshift::Column do
  context 'with a simple column' do
    before do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "character varying",
        "character_maximum_length" => "255",
        "character_octet_length"   => "1020"
      }

      @column = PostgresToRedshift::Column.new attributes: attributes
    end

    describe '#name' do
      it 'returns the column name' do
        expect(@column.name).to eq("description")
      end
    end
  end
end
