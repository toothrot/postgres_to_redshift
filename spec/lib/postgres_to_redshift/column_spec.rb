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

  describe '#name_for_copy' do
    it 'casts fields to appropriate type' do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "text",
        "character_maximum_length" => nil,
        "character_octet_length"   => "1073741824"
      }

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.name_for_copy).to eq("CAST(description AS CHARACTER VARYING(65535)) AS description")
    end

    it 'does not cast fields that do not need casting' do
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

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.name_for_copy).to eq('description')
    end
  end

  describe "#data_type_for_copy" do
    it 'casts text to character varying(65535)' do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "text",
        "character_maximum_length" => nil,
        "character_octet_length"   => "1073741824"
      }

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.data_type_for_copy).to eq("CHARACTER VARYING(65535)")
    end

    it 'casts json to character varying(65535)' do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "json",
      }

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.data_type_for_copy).to eq("CHARACTER VARYING(65535)")
    end

    it 'casts bytea to character varying(65535)' do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "bytea",
      }

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.data_type_for_copy).to eq("CHARACTER VARYING(65535)")
    end

    it 'casts money to decimal(19,2)' do
      attributes = {
        "table_catalog"            => "postgres_to_redshift",
        "table_schema"             => "public",
        "table_name"               => "films",
        "column_name"              => "description",
        "ordinal_position"         => "2",
        "column_default"           => nil,
        "is_nullable"              => "YES",
        "data_type"                => "money",
      }

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.data_type_for_copy).to eq("DECIMAL(19,2)")
    end

    it "returns the data type if no cast necessary" do
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

      column = PostgresToRedshift::Column.new attributes: attributes
      expect(column.data_type_for_copy).to eq("character varying")
    end
  end
end
