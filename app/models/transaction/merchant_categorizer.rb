class Transaction::MerchantCategorizer
  def initialize(transaction)
    @transaction = transaction
  end

  # Returns category suggestion hash or nil
  def suggest_category
    if @transaction.merchant_id.blank?
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: No merchant assigned")
      return nil
    end

    if @transaction.category_id.present?
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Already categorized (category_id: #{@transaction.category_id})")
      return nil
    end

    if @transaction.pending?
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Pending transaction, skipping")
      return nil
    end

    if @transaction.transfer?
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Transfer transaction, skipping")
      return nil
    end

    # Find past transactions with same merchant in the same family
    past_transactions = @transaction.entry.account.family.transactions
      .joins(:entry)
      .where(merchant_id: @transaction.merchant_id)
      .where.not(id: @transaction.id)
      .where.not(category_id: nil)
      .merge(Transaction.excluding_pending) # only look at posted transactions

    Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Found #{past_transactions.count} past transactions with merchant #{@transaction.merchant_id}")

    if past_transactions.empty?
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: No past transactions found, cannot suggest")
      return nil
    end

    # Calculate category frequency
    category_counts = past_transactions.group_by(&:category_id).transform_values(&:count)
    total_count = past_transactions.count

    # Find most common category
    top_category_id, top_count = category_counts.max_by { |_id, count| count }
    match_percentage = (top_count.to_f / total_count * 100).round(1)

    Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Top category #{top_category_id} appears #{top_count}/#{total_count} times (#{match_percentage}%)")

    # Only suggest if >50% threshold
    if match_percentage < 50.0
      Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Match percentage #{match_percentage}% below 50% threshold, not suggesting")
      return nil
    end

    # Determine confidence level
    confidence = if match_percentage >= 75.0
      "high"
    elsif match_percentage >= 50.0
      "medium"
    else
      "low"
    end

    category = Category.find(top_category_id)

    Rails.logger.debug("SUGG: Transaction #{@transaction.id}: Suggesting category '#{category.name}' (#{confidence} confidence)")

    {
      category_id: category.id,
      category_name: category.name,
      source: "merchant_history",
      confidence: confidence,
      merchant_history_count: total_count,
      match_percentage: match_percentage,
      suggested_at: Date.current.to_s
    }
  rescue StandardError => e
    Rails.logger.error("Failed to generate category suggestion for transaction #{@transaction.id}: #{e.message}")
    nil
  end

  # Store suggestion in transaction.extra
  def suggest_and_store!
    suggestion = suggest_category
    return false if suggestion.nil?

    existing_extra = @transaction.extra || {}

    # Don't overwrite existing suggestions
    return false if existing_extra["category_suggestion"].present?

    @transaction.update!(
      extra: existing_extra.merge("category_suggestion" => suggestion)
    )

    true
  rescue StandardError => e
    Rails.logger.error("Failed to store category suggestion for transaction #{@transaction.id}: #{e.message}")
    false
  end
end
