module Fluent
  class AnomalyDetectOutput < Output
    Fluent::Plugin.register_output('anomalydetect', self)
    
    require_relative 'change_finder'
    require 'pathname'

    config_param :outlier_term, :integer, :default => 28
    config_param :outlier_discount, :float, :default => 0.05
    config_param :smooth_term, :integer, :default => 7
    config_param :score_term, :integer, :default => 14
    config_param :score_discount, :float, :default => 0.1
    config_param :tick, :integer, :default => 60 * 5
    config_param :tag, :string, :default => "anomaly"
    config_param :add_tag_prefix, :string, :default => nil
    config_param :remove_tag_prefix, :string, :default => nil
    config_param :aggregate, :string, :default => 'all'
    config_param :target, :string, :default => nil
    config_param :store_file, :string, :default => nil
    config_param :threshold, :float, :default => -1.0
    config_param :trend, :default => nil do |val|
      case val.downcase
      when 'up'
        :up
      when 'down'
        :down
      else
        raise ConfigError, "out_anomaly treand should be 'up' or 'down'"
      end
    end

    attr_accessor :outliers
    attr_accessor :scores
    attr_accessor :record_count

    attr_accessor :outlier_bufs

    attr_accessor :records

    def configure (conf)
      super
      unless 0 < @outlier_discount and @outlier_discount < 1 
        raise Fluent::ConfigError, "discount ratio should be between (0, 1)" 
      end
      unless 0 < @score_discount and @score_discount < 1 
        raise Fluent::ConfigError, "discount ratio should be between (0, 1)"
      end
      if @outlier_term < 1
        raise Fluent::ConfigError, "outlier term should be greater than 0"
      end
      if @score_term < 1
        raise Fluent::ConfigError, "score term should be greater than 0"
      end
      if @smooth_term < 1
        raise Fluent::ConfigError, "smooth term should be greater than 0"
      end
      if @tick < 1
        raise Fluent::ConfigError, "tick timer should be greater than 1 sec"
      end
      if @store_file
        f = Pathname.new(@store_file)
        if (f.exist? && !f.writable_real?) || (!f.exist? && !f.parent.writable_real?)
          raise Fluent::ConfigError, "#{@store_file} is not writable"
        end
      end

      case @aggregate
      when 'all'
        raise Fluent::ConfigError, "anomalydetect: `tag` must be specified with aggregate all" if @tag.nil?
      when 'tag'
        raise Fluent::ConfigError, "anomalydetect: `add_tag_prefix` must be specified with aggregate tag" if @add_tag_prefix.nil?
      else
        raise Fluent::ConfigError, "anomalydetect: aggregate allows tag/all"
      end

      @tag_prefix = "#{@add_tag_prefix}." if @add_tag_prefix
      @tag_prefix_match = "#{@remove_tag_prefix}." if @remove_tag_prefix
      @tag_proc =
        if @tag_prefix and @tag_prefix_match
          Proc.new {|tag| "#{@tag_prefix}#{lstrip(tag, @tag_prefix_match)}" }
        elsif @tag_prefix_match
          Proc.new {|tag| lstrip(tag, @tag_prefix_match) }
        elsif @tag_prefix
          Proc.new {|tag| "#{@tag_prefix}#{tag}" }
        elsif @tag
          Proc.new {|tag| @tag }
        else
          Proc.new {|tag| tag }
        end

      @record_count = @target.nil?

      @records = {}
      @outliers = {}
      @outlier_bufs = {}
      @scores = {}

      @mutex = Mutex.new
    end

    def start
      super
      load_from_file
      start_watch
    rescue => e
      $log.warn "anomalydetect: #{e.class} #{e.message} #{e.backtrace.first}"
    end

    def shutdown
      super
      if @watcher
        @watcher.terminate
        @watcher.join
      end
      store_to_file
    rescue => e
      $log.warn "anomalydetect: #{e.class} #{e.message} #{e.backtrace.first}"
    end

    def start_watch
      @watcher = Thread.new(&method(:watch))
    end

    def watch
      @last_checked = Fluent::Engine.now
      loop do
        begin
          sleep 0.5
          now = Fluent::Engine.now
          if now - @last_checked >= @tick
            flush_emit(now - @last_checked)
            @last_checked = now
          end
        rescue => e
          $log.warn "anomalydetect: #{e.class} #{e.message} #{e.backtrace.first}"
        end
      end
    end

    def init_records(tags)
      records = {}
      tags.each do |tag|
        records[tag] = []
      end
      records
    end

    def flush_emit(step)
      outputs = flush
      outputs.each do |tag, output|
        emit_tag = @tag_proc.call(tag)
        Fluent::Engine.emit(emit_tag, Fluent::Engine.now, output)
      end
    end

    def flush
      flushed_records, @records = @records, init_records(tags = @records.keys)
      outputs = {}
      flushed_records.each do |tag, records|
        output = flush_each(tag, records)
        outputs[tag] = output if output
      end
      outputs
    end

    def flush_each(tag, records)
      val = get_value(records)
      outlier, score = get_score(tag, val) if val

      if score and @threshold < 0 or (@threshold >= 0 and score > @threshold)
        case @trend
        when :up
          return nil if val < @outliers[tag].mu
        when :down
          return nil if val > @outliers[tag].mu
        end
        {"outlier" => outlier, "score" => score, "target" => val}
      else
        nil
      end
    end

    def get_value(records)
      if @record_count
        records.size.to_f
      else
        compacted_records = records.map {|record| record[@target] }.compact
        return nil if compacted_records.empty?
        compacted_records.inject(:+).to_f / compacted_records.size
      end
    end

    def get_score(tag, val)
      @outlier_bufs[tag] ||= []
      @outliers[tag]     ||= ChangeFinder.new(@outlier_term, @outlier_discount)
      @scores[tag]       ||= ChangeFinder.new(@score_term, @score_discount)

      outlier = @outliers[tag].next(val)

      @outlier_bufs[tag].push outlier
      @outlier_bufs[tag].shift if @outlier_bufs[tag].size > @smooth_term
      outlier_avg = @outlier_bufs[tag].empty? ? 0.0 : @outlier_bufs[tag].inject(:+).to_f / @outlier_bufs[tag].size

      score = @scores[tag].next(outlier_avg)

      $log.debug "out_anomalydetect:#{Thread.current.object_id} tag:#{tag} val:#{val} outlier:#{outlier} outlier_buf:#{@outlier_bufs[tag]} score:#{score}"

      [outlier, score]
    end

    def push_records(tag, records)
      @mutex.synchronize do
        @records[tag] ||= []
        @records[tag].concat(records)
      end
    end

    def emit(tag, es, chain)
      records = es.map { |time, record| record }
      if @aggregate == 'all'
        push_records(:all, records)
      else
        push_records(tag, records)
      end

      chain.next
    rescue => e
      $log.warn "anomalydetect: #{e.class} #{e.message} #{e.backtrace.first}"
    end

    def load_from_file
      return unless @store_file
      f = Pathname.new(@store_file)
      return unless f.exist?

      begin
        f.open('rb') do |f|
          stored = Marshal.load(f)
          if (( stored[:outlier_term]     == @outlier_term ) &&
              ( stored[:outlier_discount] == @outlier_discount ) &&
              ( stored[:score_term]       == @score_term ) &&
              ( stored[:score_discount]   == @score_discount ) &&
              ( stored[:smooth_term]      == @smooth_term ) &&
              ( stored[:aggregate]        == @aggregate ))
          then
            @outliers     = stored[:outliers]
            @outlier_bufs = stored[:outlier_bufs]
            @scores       = stored[:scores]
          else
            $log.warn "anomalydetect: configuration param was changed. ignore stored data"
          end
        end
      rescue => e
        $log.warn "anomalydetect: Can't load store_file #{e}"
      end
    end

    def store_to_file
      return unless @store_file
      begin
        Pathname.new(@store_file).open('wb') do |f|
          Marshal.dump({
            :outliers         => @outliers,
            :outlier_bufs     => @outlier_bufs,
            :scores           => @scores,
            :outlier_term     => @outlier_term,
            :outlier_discount => @outlier_discount,
            :score_term       => @score_term,
            :score_discount   => @score_discount,
            :smooth_term      => @smooth_term,
            :aggregate        => @aggregate,
          }, f)
        end
      rescue => e
        $log.warn "anomalydetect: Can't write store_file #{e}"
      end
    end

    private

    def lstrip(string, substring)
      string.index(substring) == 0 ? string[substring.size..-1] : string
    end
  end
end
