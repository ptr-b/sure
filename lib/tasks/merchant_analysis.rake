namespace :merchants do
  desc "Analyze merchants with uncategorized transactions to identify low-hanging fruit"
  task analyze_uncategorized: :environment do
    puts "=" * 80
    puts "Merchant Categorization Analysis"
    puts "=" * 80
    puts ""

    Family.find_each do |family|
      puts "Family: #{family.id}"
      puts "-" * 80

      # Get all transactions grouped by merchant
      merchant_stats = family.transactions
        .where.not(merchant_id: nil)
        .merge(Transaction.excluding_pending)
        .group(:merchant_id)
        .select(
          :merchant_id,
          "COUNT(*) as total_count",
          "COUNT(category_id) as categorized_count",
          "COUNT(*) FILTER (WHERE category_id IS NULL) as uncategorized_count"
        )
        .having("COUNT(*) FILTER (WHERE category_id IS NULL) > 0")
        .order(Arel.sql("COUNT(*) FILTER (WHERE category_id IS NULL) DESC"))

      if merchant_stats.empty?
        puts "No merchants with uncategorized transactions found."
        puts ""
        next
      end

      total_uncategorized = 0
      merchant_data = []

      merchant_stats.each do |stat|
        merchant = Merchant.find(stat.merchant_id)
        total = stat.total_count
        categorized = stat.categorized_count
        uncategorized = stat.uncategorized_count
        percent_uncategorized = (uncategorized.to_f / total * 100).round(1)

        total_uncategorized += uncategorized

        merchant_data << {
          name: merchant.name,
          total: total,
          categorized: categorized,
          uncategorized: uncategorized,
          percent: percent_uncategorized,
          suggestionable: categorized > 0 && (categorized.to_f / total) >= 0.5
        }
      end

      # Display results
      puts ""
      puts "Top merchants by uncategorized transaction count:"
      puts ""
      printf("%-40s %8s %12s %14s %10s %15s\n",
             "Merchant", "Total", "Categorized", "Uncategorized", "% Uncat", "Auto-Suggest?")
      puts "-" * 100

      merchant_data.first(20).each do |data|
        suggestionable = data[:suggestionable] ? "Yes (>50%)" : "No"
        printf("%-40s %8d %12d %14d %9.1f%% %15s\n",
               data[:name].truncate(40),
               data[:total],
               data[:categorized],
               data[:uncategorized],
               data[:percent],
               suggestionable)
      end

      puts ""
      puts "Summary:"
      puts "  Total merchants with uncategorized transactions: #{merchant_data.count}"
      puts "  Total uncategorized transactions: #{total_uncategorized}"
      puts "  Merchants ready for auto-suggestion (>50% categorized): #{merchant_data.count { |d| d[:suggestionable] }}"
      puts "  Merchants needing manual categorization: #{merchant_data.count { |d| !d[:suggestionable] }}"
      puts ""
    end

    puts "=" * 80
    puts "Analysis complete!"
    puts "=" * 80
  end
end
