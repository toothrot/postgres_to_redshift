# table_catalog            | postgres_to_redshift
# table_schema             | public
# table_name               | films
# column_name              | description
# ordinal_position         | 2
# column_default           |
# is_nullable              | YES
# data_type                | character varying
# character_maximum_length | 255
# character_octet_length   | 1020
# numeric_precision        |
# numeric_precision_radix  |
# numeric_scale            |
# datetime_precision       |
# interval_type            |
# interval_precision       |
# character_set_catalog    |
# character_set_schema     |
# character_set_name       |
# collation_catalog        |
# collation_schema         |
# collation_name           |
# domain_catalog           |
# domain_schema            |
# domain_name              |
# udt_catalog              | postgres_to_redshift
# udt_schema               | pg_catalog
# udt_name                 | varchar
# scope_catalog            |
# scope_schema             |
# scope_name               |
# maximum_cardinality      |
# dtd_identifier           | 2
# is_self_referencing      | NO
# is_identity              | NO
# identity_generation      |
# identity_start           |
# identity_increment       |
# identity_maximum         |
# identity_minimum         |
# identity_cycle           |
# is_generated             | NEVER
# generation_expression    |
# is_updatable             | YES
#
class PostgresToRedshift::Column
  attr_accessor :attributes

  def initialize(attributes: )
    self.attributes = attributes
  end

  def name
    attributes["column_name"]
  end
end
