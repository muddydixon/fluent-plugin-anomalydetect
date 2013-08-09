module Fluent
  class AnomalyDetectOutput < Output
    Fluent::Plugin.register_output('anomalydetect', self)
    
    require 'fluent/plugin/change_finder'
    require 'pathname'

    config_param :outlier_term, :integer, :default => 28
    config_param :outlier_discount, :float, :default => 0.05
    config_param :smooth_term, :integer, :default => 7
    config_param :score_term, :integer, :default => 14
    config_param :score_discount, :float, :default => 0.1
    config_param :tick, :integer, :default => 60 * 5
    config_param :tag, :string, :default => "anomaly"
    config_param :target, :string, :default => nil
    config_param :store_file, :string, :default => nil
    config_param :threshold, :float, :default => -1.0

    attr_accessor :outlier
    attr_accessor :score
    attr_accessor :record_count

    attr_accessor :outlier_buf

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
      @outlier_buf = []
      @outlier  = ChangeFinder.new(@outlier_term, @outlier_discount)
      @score    = ChangeFinder.new(@score_term, @score_discount)

      @mutex = Mutex.new

      @record_count = @target.nil?
    end

    def start
      super
      load_from_file
      init_records
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
              ( stored[:smooth_term]      == @smooth_term ))
          then
            @outlier  = stored[:outlier]
            @outlier_buf = stored[:outlier_buf]
            @score    = stored[:score]
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
            :outlier          => @outlier,
            :outlier_buf         => @outlier_buf,
            :score            => @score,
            :outlier_term     => @outlier_term,
            :outlier_discount => @outlier_discount,
            :score_term       => @score_term,
            :score_discount   => @score_discount,
            :smooth_term      => @smooth_term,
          }, f)
        end
      rescue => e
        $log.warn "anomalydetect: Can't write store_file #{e}"
      end
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

    def init_records
      @records = []
    end

    def flush_emit(step)
      output = flush
      if output
        Fluent::Engine.emit(@tag, Fluent::Engine.now, output)
      end
    end

    def flush
      flushed, @records = @records, init_records

      val = if @record_count
              flushed.size
            else
              flushed.inject(0.0) { |sum, record| sum += record[@target].to_f if record[@target] } / flushed.size
            end

      outlier = @outlier.next(val)
      @outlier_buf.push outlier
      @outlier_buf.shift if @outlier_buf.size > @smooth_term
      score = @score.next(@outlier_buf.inject(0) { |sum, v| sum += v } / @outlier_buf.size)

      $log.debug "out_anomalydetect:#{Thread.current.object_id} flushed:#{flushed} val:#{val} outlier:#{outlier} outlier_buf:#{@outlier_buf} score:#{score}"
      if @threshold < 0 or (@threshold >= 0 and score > @threshold)
        {"outlier" => outlier, "score" => score, "target" => val}
      else
        nil
      end
    end

    def tick_time(time)
      (time - time % @tick).to_s
    end

    def push_records(records)
      @mutex.synchronize do
        @records.concat(records)
      end
    end

    def emit(tag, es, chain)
      records = es.map { |time, record| record }
      push_records records

      chain.next
    rescue => e
      $log.warn "anomalydetect: #{e.class} #{e.message} #{e.backtrace.first}"
    end
  end
end
