require "digest/md5"

class TransactionImport < Import
  def import!
    transaction do
      mappings.each(&:create_mappable!)

      new_transactions = []
      updated_entries = []
      claimed_entry_ids = Set.new # Track entries we've already claimed in this import

      rows.each_with_index do |row, index|
        mapped_account = if account
          account
        else
          mappings.accounts.mappable_for(row.account)
        end

        # Guard against nil account - this happens when an account name in CSV is not mapped
        if mapped_account.nil?
          row_number = index + 1
          account_name = row.account.presence || "(blank)"
          error_message = "Row #{row_number}: Account '#{account_name}' is not mapped to an existing account. " \
                         "Please map this account in the import configuration."
          errors.add(:base, error_message)
          raise Import::MappingError, error_message
        end

        category = mappings.categories.mappable_for(row.category)
        tags = row.tags_list.map { |tag| mappings.tags.mappable_for(tag) }.compact

        # Use adapter for merchant creation and duplicate detection
        adapter = Account::ProviderImportAdapter.new(mapped_account)

        # Create merchant from transaction name (similar to Enable Banking integration)
        merchant = if row.name.present?
          merchant_name = row.name.to_s.strip
          unless merchant_name.blank?
            merchant_id = Digest::MD5.hexdigest(merchant_name.downcase)

            begin
              adapter.find_or_create_merchant(
                provider_merchant_id: "csv_merchant_#{merchant_id}",
                name: merchant_name,
                source: "csv"
              )
            rescue ActiveRecord::RecordInvalid => e
              Rails.logger.error "TransactionImport - Failed to create merchant '#{merchant_name}': #{e.message}"
              nil
            end
          end
        end

        # Use account's currency when no currency column was mapped in CSV, with family currency as fallback
        effective_currency = currency_col_label.present? ? row.currency : (mapped_account.currency.presence || family.currency)

        # Check for duplicate transactions using the adapter's deduplication logic
        # Pass claimed_entry_ids to exclude entries we've already matched in this import
        # This ensures identical rows within the CSV are all imported as separate transactions
        duplicate_entry = adapter.find_duplicate_transaction(
          date: row.date_iso,
          amount: row.signed_amount,
          currency: effective_currency,
          name: row.name,
          exclude_entry_ids: claimed_entry_ids
        )

        if duplicate_entry
          # Update existing transaction instead of creating a new one
          duplicate_entry.transaction.category = category if category.present?
          duplicate_entry.transaction.tags = tags if tags.any?
          duplicate_entry.transaction.merchant = merchant if merchant.present?
          duplicate_entry.notes = row.notes if row.notes.present?
          duplicate_entry.import = self
          duplicate_entry.import_locked = true  # Protect from provider sync overwrites
          updated_entries << duplicate_entry
          claimed_entry_ids.add(duplicate_entry.id)
        else
          # Create new transaction (no duplicate found)
          # Mark as import_locked to protect from provider sync overwrites
          new_transactions << Transaction.new(
            category: category,
            tags: tags,
            merchant: merchant,
            entry: Entry.new(
              account: mapped_account,
              date: row.date_iso,
              amount: row.signed_amount,
              name: row.name,
              currency: effective_currency,
              notes: row.notes,
              import: self,
              import_locked: true
            )
          )
        end
      end

      # Save updated entries first
      updated_entries.each do |entry|
        entry.transaction.save!
        entry.save!
      end

      # Bulk import new transactions
      Transaction.import!(new_transactions, recursive: true) if new_transactions.any?

      # Generate category suggestions for newly imported uncategorized transactions
      new_transactions.each do |transaction|
        next if transaction.category_id.present?
        next if transaction.merchant_id.blank?

        begin
          Transaction::MerchantCategorizer.new(transaction).suggest_and_store!
        rescue StandardError => e
          Rails.logger.warn("Failed to generate category suggestion for transaction #{transaction.id}: #{e.message}")
        end
      end
    end
  end

  def required_column_keys
    %i[date amount]
  end

  def column_keys
    base = %i[date amount name currency category tags notes]
    base.unshift(:account) if account.nil?
    base
  end

  def mapping_steps
    base = [ Import::CategoryMapping, Import::TagMapping ]
    base << Import::AccountMapping if account.nil?
    base
  end

  def selectable_amount_type_values
    return [] if entity_type_col_label.nil?

    csv_rows.map { |row| row[entity_type_col_label] }.uniq
  end

  def csv_template
    template = <<-CSV
      date*,amount*,name,currency,category,tags,account,notes
      05/15/2024,-45.99,Grocery Store,USD,Food,groceries|essentials,Checking Account,Monthly grocery run
      05/16/2024,1500.00,Salary,,Income,,Main Account,
      05/17/2024,-12.50,Coffee Shop,,,coffee,,
    CSV

    csv = CSV.parse(template, headers: true)
    csv.delete("account") if account.present?
    csv
  end
end
