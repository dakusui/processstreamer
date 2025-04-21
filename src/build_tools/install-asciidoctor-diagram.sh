#!/usr/bin/env bash
set -eu

[[ -e .dependencies/gem/ruby ]] && mkdir -p .dependencies/gem/ruby
gem install concurrent-ruby -i .dependencies/gem/ruby
gem install asciidoctor-diagram --version=2.3.0 -i .dependencies/gem/ruby
