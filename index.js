require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const cron = require('node-cron');
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

// ENV variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const HP_USERNAME = process.env.HP_USERNAME;
const HP_PASSWORD = process.env.HP_PASSWORD;
const X_API_KEY = process.env.X_API_KEY;
const HP_AUTH_URL = process.env.HP_AUTH_URL;
const HP_DATA_URL = process.env.HP_DATA_URL;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

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
    if (!accessToken) throw new Error('No access token');

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

    console.log("\ud83d\udce6 API Raw Response:", JSON.stringify(dataRes.data, null, 2));

    // \u2705 Correct extraction from 'programs' key
    const records = dataRes.data.programs;

    if (!Array.isArray(records) || records.length === 0) {
      console.log('\u26a0\ufe0f No records found');
      return;
    }

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
        console.error('\u274c Insert error:', error);
        continue;
      }

      for (const pay of item.payments || []) {
        await supabase.from('payments').insert([{
          program_id: insertedProgram.id,
          bonus_usd: parseFloat(pay.bonusUSD),
          bonus_eur: parseFloat(pay.bonusEUR),
          bonus_lc: parseFloat(pay.bonusLC),
          payment_date: new Date(pay.paymentdate),
          reference_id: pay.referenceID,
          payment_document: pay.paymentDocument
        }]);
      }
    }

    console.log('\u2705 All data inserted successfully');
  } catch (err) {
    console.error('\u274c Error:', err.response?.data || err.message);
  }
}

// Schedule daily at 10 AM
cron.schedule('0 10 * * *', () => {
  console.log('\u23f0 Scheduled fetch at 10AM');
  fetchAndStoreData();
});

// Manual run
fetchAndStoreData();

// Express app for manual trigger
app.get('/', (req, res) => {
  res.send('\ud83d\udfe2 HP Data Loader is running');
});

app.get('/run-now', async (req, res) => {
  try {
    await fetchAndStoreData();
    res.send('\u2705 Data fetch completed and pushed to Supabase');
  } catch (error) {
    console.error('\u274c Error during manual run:', error.message);
    res.status(500).send('\u274c Failed to run data fetch');
  }
});

app.listen(PORT, () => {
  console.log(`\ud83d\ude80 Server is listening on port ${PORT}`);
});