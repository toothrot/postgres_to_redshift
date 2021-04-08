# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'postgres_to_redshift/version'

Gem::Specification.new do |spec|
  spec.name          = "postgres_to_redshift"
  spec.version       = PostgresToRedshift::VERSION
  spec.authors       = ["Alex Rakoczy"]
  spec.email         = ["arakoczy@gmail.com"]
  spec.summary       = %q{Load postgres databases into Amazon Redshift}
  spec.description   = %q{Load postgres databases into Amazon Redshift. It's designed to work on Heroku Scheduler, or other *nix/BSD hosts.}
  spec.homepage      = "https://github.com/toothrot/postgres_to_redshift"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 2.2"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_runtime_dependency "pg", "~> 1.2"
  spec.add_runtime_dependency "aws-sdk-s3", "~> 1.93"
end
