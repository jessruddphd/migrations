select
        aw.deposit_id,
        aw.fanduel_user_id,
        aw.product_name,
        aw.product_id,
        aw.deposit_ts,
        aw.deposit_ts::date as deposit_date_date,
        aw.deposit_complete_ts,
        aw.payment_method,
        aw.deposit_amount,
        aw.payment_status,
        case
            when bt.merchant_account_id = 'fanduelinc_instant' then 'Braintree'
            when bt.merchant_account_id = 'fanduellitleusd' then 'WorldPay'
            when bt.merchant_account_id = 'fanduellitleusd_2' then 'WorldPay'
            when bt.merchant_account_id = 'fanduelfd' then 'firstdata'
            when bt.merchant_account_id = 'fanduelusd' then 'fanduel usd'
            when bt.merchant_account_id = 'fanduelchaseusd' then 'Chase'
            when bt.merchant_account_id = 'fanduel_sportswagering_tf' then 'Transfund'
            when bt.merchant_account_id = 'transfund_skillgames' then 'Transfund'
            when bt.merchant_account_id = 'fanduel_sportswagering_tfnew' then 'Transfund'
            when bt.merchant_account_id = 'fanduel_sportswagering_wp' then 'WorldPay'
            when bt.merchant_account_id ilike '%wp%' then 'WorldPay'
            else 'Braintree'
        end as processor,
        bt.amount_authorized,
        bt.amount_submitted_for_settlement,
        bt.currency_iso_code,
        bt.payment_instrument_type,
        case
            when bt.card_type ilike '%expr%' then 'american express'
            when bt.card_type ilike '%master%' then 'mastercard'
            when bt.card_type ilike '%discov%' then 'discover'
            when bt.card_type ilike '%visa%' then 'visa'
            else bt.card_type
        end as network,
        bt.card_type,
        bt.issuing_bank,
        bt.transaction_status,
        bt.processor_response_code,
        bt.processor_response_text,
        bt.processor_response_type,
        bt.country_of_issuance,
        bt.processed_with_network_token,
        bt.healthcare,
        bt.bin,
        bt.debit,
        bt.gateway_rejection_reason,
        bt.requested_amount,
        case
            when processor = 'Braintree' then 0.01
            when processor = 'WorldPay' then 0.05
            when processor = 'Transfund' then 0.06
        end as fee_fixed,
        case
            when processor = 'Braintree' and bt.transaction_status = 'settled' then (0.0001 * aw.deposit_amount)
            when processor = 'WorldPay' and bt.transaction_status = 'settled' then (0.001 * aw.deposit_amount)
            when processor = 'Transfund' and bt.transaction_status = 'settled' then (0.0015 * aw.deposit_amount)
        end as fee_variable,
        fee_variable + fee_fixed as fees_total,
        vud.state as kyc_state
    from foundation_views.financial.deposits aw
        left join foundation_views.financial.braintree_all_transactions bt
            on bt.transaction_id = aw.processor_transaction_id
        left join foundation_views.account.verified_user_details vud
            on vud.fanduel_user_id = aw.fanduel_user_id
    where aw.payment_method = 'braintree'
      and (bt.payment_instrument_type = 'credit_card' or bt.payment_instrument_type = 'apple_pay_card')
      and aw.deposit_ts::date >= '2023-01-01'