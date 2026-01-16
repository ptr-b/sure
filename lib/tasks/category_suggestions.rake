namespace :transactions do
  desc "Generate category suggestions for uncategorized transactions based on merchant history"
  task suggest_categories: :environment do
    puts "=" * 80
    puts "Category Suggestion Generator"
    puts "=" * 80
    puts ""

    # Enable debug logging to see SUGG: prefixed logs
    original_log_level = Rails.logger.level
    Rails.logger.level = :debug

    Family.find_each do |family|
      puts "Processing family: #{family.id}"
      puts "-" * 80

      candidate_transactions = family.transactions
        .where(category_id: nil)
        .where.not(merchant_id: nil)
        .merge(Transaction.excluding_pending)

      total_candidates = candidate_transactions.count
      puts "SUGG: Found #{total_candidates} uncategorized transactions with merchants"

      next if total_candidates.zero?

      suggested_count = 0
      skipped_count = 0

      candidate_transactions.find_each do |transaction|
        suggester = Transaction::MerchantCategorizer.new(transaction)
        if suggester.suggest_and_store!
          suggested_count += 1
        else
          skipped_count += 1
        end
      rescue StandardError => e
        puts "SUGG: ERROR - Failed to process transaction #{transaction.id}: #{e.message}"
        skipped_count += 1
      end

      puts ""
      puts "Summary for family #{family.id}:"
      puts "  Total candidates: #{total_candidates}"
      puts "  Suggestions created: #{suggested_count}"
      puts "  Skipped (no pattern found): #{skipped_count}"
      puts ""
    end

    # Restore original log level
    Rails.logger.level = original_log_level

    puts "=" * 80
    puts "Category suggestion generation complete!"
    puts "=" * 80
  end
end
