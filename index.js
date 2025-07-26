async function fetchAndStoreData() {
  try {
    // Step 1: Get token
    const auth = Buffer.from(`${HP_USERNAME}:${HP_PASSWORD}`).toString('base64');
    const tokenRes = await axios.post(HP_AUTH_URL, null, {
      headers: {
        Authorization: `Basic ${auth}`,
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    const accessToken = tokenRes.data.access_token;
    if (!accessToken) throw new Error('No access token received');

    // Step 2: Get data
    const dataRes = await axios.get(HP_DATA_URL, {
      headers: {
        token: accessToken,
        'x-api-key': X_API_KEY
      },
      params: {
        partnerid: '70641239',
        quarter: 'Q1-24',
        page: 1,
        pageSize: 500
      }
    });

    const records = dataRes.data.programs;
    console.log("✅ Records fetched from API:", records?.length || 0);
    if (!Array.isArray(records) || records.length === 0) {
      console.log("⚠️ No data returned from HP API. Check parameters or credentials.");
      return;
    }

    // Insert into Supabase
    for (const item of records) {
      const { data: insertedProgram, error } = await supabase
        .from('programs')
        .insert([{
          program_quarter: item.programQuarter,
          group_partner_id: item.groupPartnerID,
          partner_location_id: item.partnerLocationID,
          partner_name: item.partnerName,
          iso_country_code: item.isoCountryCode,
          scheme: item.scheme,
          category: item.category,
          bu: item.bu,
          sub_bu: item.subBU,
          type: item.type,
          target_usd: parseFloat(item.targetUSD),
          bonus_usd: parseFloat(item.bonusUSD),
          bonus_lc: parseFloat(item.bonusLC),
          status: item.status,
          lc_currency: item.lcCurrency,
          rate_usd_lc: parseFloat(item.rateUSDLC),
          program_group: item.programGroup
        }])
        .select()
        .single();

      if (error) {
        console.error('❌ Insert error in programs:', error);
        continue;
      }

      for (const pay of item.payments || []) {
        const { error: payErr } = await supabase.from('payments').insert([{
          program_id: insertedProgram.id,
          bonus_usd: parseFloat(pay.bonusUSD),
          bonus_eur: parseFloat(pay.bonusEUR),
          bonus_lc: parseFloat(pay.bonusLC),
          payment_date: new Date(pay.paymentdate),
          reference_id: pay.referenceID,
          payment_document: pay.paymentDocument
        }]);

        if (payErr) {
          console.error("❌ Insert error in payments:", payErr);
        }
      }
    }

    console.log('✅ All data inserted into Supabase successfully');

  } catch (err) {
    console.error('❌ Fetch/Insert Error:', err.response?.data || err.message);
  }
}
