namespace :csv do
  # Supported import types
  IMPORT_TYPES = {
    "transaction" => "TransactionImport",
    "trade" => "TradeImport",
    "account" => "AccountImport",
    "category" => "CategoryImport",
    "rule" => "RuleImport",
    "mint" => "MintImport"
  }.freeze

  desc "Import from a CSV file (type: transaction, trade, account, category, rule, mint)"
  task :import, [ :file_path, :user_email, :type, :account_name ] => :environment do |_t, args|
    file_path = args[:file_path]
    user_email = args[:user_email]
    import_type = args[:type] || "transaction"
    account_name = args[:account_name]

    # Validate arguments
    unless file_path.present?
      puts "❌ Usage: bin/rails 'csv:import[path/to/file.csv,user@example.com,type,Account Name]'"
      puts "   type: #{IMPORT_TYPES.keys.join(', ')} (default: transaction)"
      puts "   account_name is optional - if omitted, uses the 'account' column from CSV"
      exit 1
    end

    # Validate import type
    unless IMPORT_TYPES.key?(import_type)
      puts "❌ Invalid import type: #{import_type}"
      puts "   Valid types: #{IMPORT_TYPES.keys.join(', ')}"
      exit 1
    end

    unless File.exist?(file_path)
      puts "❌ File not found: #{file_path}"
      exit 1
    end

    # Find user and family
    user = User.find_by(email: user_email)
    unless user
      puts "❌ User not found: #{user_email}"
      puts "   Available users: #{User.pluck(:email).join(', ')}"
      exit 1
    end

    family = user.family
    puts "📁 Importing for family: #{family.name} (user: #{user_email})"

    # Optionally find account
    account = nil
    if account_name.present?
      account = family.accounts.find_by("LOWER(name) = ?", account_name.downcase)
      unless account
        puts "❌ Account not found: #{account_name}"
        puts "   Available accounts: #{family.accounts.pluck(:name).join(', ')}"
        exit 1
      end
      puts "📊 Targeting account: #{account.name}"
    end

    # Read CSV file
    csv_content = File.read(file_path)
    puts "📄 Read #{csv_content.lines.count} lines from #{file_path}"

    # Create import
    import_class = IMPORT_TYPES[import_type]
    import = family.imports.create!(
      type: import_class,
      raw_file_str: csv_content,
      col_sep: ",",
      account: account
    )
    puts "📦 Created #{import_class}"

    puts "🔍 Auto-detecting columns..."
    import.auto_detect_columns!
    import.reload

    detected = []
    detected << "date=#{import.date_col_label}" if import.date_col_label.present?
    detected << "amount=#{import.amount_col_label}" if import.amount_col_label.present?
    detected << "name=#{import.name_col_label}" if import.name_col_label.present?
    detected << "category=#{import.category_col_label}" if import.category_col_label.present?
    detected << "tags=#{import.tags_col_label}" if import.tags_col_label.present?
    detected << "account=#{import.account_col_label}" if import.account_col_label.present?
    detected << "notes=#{import.notes_col_label}" if import.notes_col_label.present?
    detected << "currency=#{import.currency_col_label}" if import.currency_col_label.present?
    # Trade-specific columns
    detected << "qty=#{import.qty_col_label}" if import.qty_col_label.present?
    detected << "ticker=#{import.ticker_col_label}" if import.ticker_col_label.present?
    detected << "price=#{import.price_col_label}" if import.price_col_label.present?

    if detected.any?
      puts "   Detected: #{detected.join(', ')}"
    else
      puts "   ⚠️  No columns auto-detected. CSV headers: #{import.csv_headers.join(', ')}"
      puts "   Make sure your CSV has headers matching the expected column names"
      import.destroy
      exit 1
    end

    # Check required columns based on import type
    required_cols = import.required_column_keys
    missing_cols = required_cols.select { |col| import.send("#{col}_col_label").blank? }

    if missing_cols.any?
      puts "❌ Missing required columns: #{missing_cols.join(', ')}"
      puts "   CSV headers: #{import.csv_headers.join(', ')}"
      import.destroy
      exit 1
    end

    # Set reasonable defaults for import configuration
    import.update!(
      date_format: "%m/%d/%Y",
      signage_convention: "inflows_negative",
      amount_type_strategy: "signed_amount"
    )

    # Generate rows from CSV
    puts "📝 Generating import rows..."
    import.generate_rows_from_csv
    import.reload
    puts "   Generated #{import.rows_count} rows"

    # Sync mappings (for categories, tags, accounts)
    puts "🔗 Syncing mappings..."
    import.sync_mappings

    # Check if import is ready
    unless import.publishable?
      puts "⚠️  Import has validation issues:"
      import.rows.each do |row|
        next if row.valid?
        puts "   Row #{row.id}: #{row.errors.full_messages.join(', ')}"
      end
      puts "\n   Import ID: #{import.id} - you can continue in the UI"
      exit 1
    end

    # Publish the import
    puts "🚀 Publishing import..."
    import.publish

    if import.complete?
      item_name = case import_type
      when "transaction" then "transactions"
      when "trade" then "trades"
      when "account" then "accounts"
      when "category" then "categories"
      when "rule" then "rules"
      else "items"
      end
      puts "✅ Import complete! #{import.rows_count} #{item_name} imported."
    else
      puts "❌ Import failed: #{import.error}"
      exit 1
    end
  end

  desc "List recent imports"
  task :list, [ :user_email ] => :environment do |_t, args|
    user_email = args[:user_email]

    user = User.find_by(email: user_email)
    unless user
      puts "❌ User not found: #{user_email}"
      exit 1
    end

    imports = user.family.imports.order(created_at: :desc).limit(10)

    if imports.empty?
      puts "No imports found."
    else
      puts "Recent imports for #{user_email}:"
      puts "-" * 80
      imports.each do |import|
        puts "#{import.id} | #{import.type.ljust(20)} | #{import.status.ljust(10)} | #{import.rows_count} rows | #{import.created_at.strftime('%Y-%m-%d %H:%M')}"
      end
    end
  end
end
