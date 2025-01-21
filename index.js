// Required dependencies
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { Readable } = require("stream");
const hubspot = require("@hubspot/api-client");
const dotenv = require("dotenv");
const xlsx = require("xlsx");

// Load environment variables
dotenv.config();

const {
  ACCESS_KEY,
  SECRET_KEY,
  CUSTOM_OBJECT_ID,
  HUBSPOT_ACCESS_TOKEN,
  ASSOCIATION_TYPE_ID,
} = process.env;

const hubspotClient = new hubspot.Client({
  accessToken: HUBSPOT_ACCESS_TOKEN,
});

const s3Client = new S3Client({
  region: "ap-south-1",
  credentials: {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
  },
});

const BUCKET_NAME = "hubspot-si";
const OBJECT_KEY = "Client Audit Reports with correct dates.csv";

// Helper function
const chunkArray = (array, chunkSize) => {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
};

// const fetchCsvFromS3 = async () => {
//   try {
//     const command = new GetObjectCommand({
//       Bucket: BUCKET_NAME,
//       Key: OBJECT_KEY,
//     });
//     const response = await s3Client.send(command);
//     return response.Body;
//   } catch (error) {
//     console.error("Error fetching CSV from S3:", error);
//     throw error;
//   }
// };

const fetchExcelFromLocal = async (filePath) => {
  try {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];

    const jsonData = xlsx.utils.sheet_to_json(worksheet);

    return jsonData;
  } catch (error) {
    console.error("Error reading Excel from local:", error);
    throw error;
  }
};

const createBrandsInObjectInBatches = async (customObjectType, batchInput) => {
  try {
    const batchedInputs = chunkArray(batchInput.inputs, 100);
    for (const batch of batchedInputs) {
      await hubspotClient.crm.objects.batchApi.create(customObjectType, {
        inputs: batch,
      });
    }
  } catch (e) {
    if (e.message === "HTTP request failed") {
      console.error("Error details:", JSON.stringify(e.response, null, 2));
    } else {
      console.error("Unexpected error:", e);
    }
  }
};

const fetchAllCompanies = async (limit, after) => {
  const PublicObjectSearchRequest = {
    limit: limit,
    after: after,
    properties: ["order_code", "order_id", "hs_object_id"],
  };

  try {
    const apiResponse = await hubspotClient.crm.companies.searchApi.doSearch(
      PublicObjectSearchRequest
    );
    return {
      total: apiResponse?.total,
      data: apiResponse.results,
    };
  } catch (e) {
    e.message === "HTTP request failed"
      ? console.error(JSON.stringify(e.response, null, 2))
      : console.error(e);
  }
};

const createCompaniesInBatches = async (BatchInput) => {
  try {
    await hubspotClient.crm.companies.batchApi.create(BatchInput);
  } catch (e) {
    if (e.message === "HTTP request failed") {
      console.error("Error details:", JSON.stringify(e.response, null, 2));
    } else {
      console.error("Unexpected error:", e);
    }
  }
};

const processCsvData = async (readableStream) => {
  const rows = [];
  let header = [];

  return new Promise((resolve, reject) => {
    let remaining = "";

    readableStream
      .on("data", (chunk) => {
        remaining += chunk.toString();

        const lines = remaining.split("\n");
        lines.slice(0, -1).forEach((line, index) => {
          const values = line.split(",");

          if (index === 0 && header.length === 0) {
            header = values.map((key) => key.trim());
          } else if (values.length === header.length) {
            const row = header.reduce((acc, key, i) => {
              acc[key] = values[i].trim();
              return acc;
            }, {});
            rows.push(row);
          }
        });

        remaining = lines[lines.length - 1];
      })
      .on("end", () => {
        if (remaining) {
          const values = remaining.split(",");
          if (values.length === header.length) {
            const row = header.reduce((acc, key, i) => {
              acc[key] = values[i].trim();
              return acc;
            }, {});
            rows.push(row);
          }
        }

        resolve(rows);
      })
      .on("error", (error) => {
        console.error("Error processing CSV data:", error);
        reject(error);
      });
  });
};

const cleanRecord = (record) => {
  const sanitizedRecord = {};

  for (const key in record) {
    let value = record[key];

    if (typeof value === "string") {
      value = value.replace(/\\\"/g, '"').replace(/^"|"$/g, "");

      if (value.includes("%")) {
        value = parseFloat(value.replace("%", "")).toFixed(2) / 100;
      } else if (!isNaN(Date.parse(value)) && isNaN(value)) {
        const date = new Date(value);
        value = date.toISOString();
      } else if (!isNaN(value) && isFinite(value)) {
        value = parseFloat(value).toString();
      } else if (value === "NULL") {
        value = -1;
      }
    }

    sanitizedRecord[key] = value;
  }

  return sanitizedRecord;
};

const findRecordId = (companyArray, orderCode, orderId) => {
  for (const record of companyArray) {
    if (record.order_code == orderCode || record.order_id == orderId) {
      return record.record_id;
    }
  }

  return null;
};

(main = async () => {
  try {
    const companyArray = [];
    let after = 0;
    let count = 0;

    let response = await fetchAllCompanies(200, after);

    do {
      response?.data.forEach((item) => {
        const obj = {
          order_code: item.properties.order_code,
          order_id: item.properties.order_id,
          record_id: item.properties.hs_object_id,
        };
        companyArray.push(obj);
      });

      count++;
      after += 200;

      const total = response?.total || 0;
      const remaining = total - after;

      if (remaining > 0) {
        response = await fetchAllCompanies(200, after);
      } else {
        response = null;
      }
    } while (response?.data?.length);

    const localStream = await fetchExcelFromLocal(
      "/home/ankit/Desktop/Single-Interface/Client Audit Nov Report 230 clients.xlsx"
    );
    // const s3Stream = await fetchCsvFromS3()
    // const s3CsvArray = await processCsvData(Readable.from(s3Stream));

    const s3CsvArray = localStream;

    const companyOrderSet = new Set(
      companyArray.flatMap((item) => [item.order_code, item.order_id])
    );

    const filteredS3Array = s3CsvArray.filter((item) => {
      return (
        !companyOrderSet.has(item.order_code) &&
        !companyOrderSet.has(item.order_id)
      );
    });

    const batchInput = {
      inputs: filteredS3Array.map((item) => {
        return {
          properties: {
            name: item.brand_name,
            order_id: item.order_id,
            order_code: item.order_code,
          },
        };
      }),
    };

    if (batchInput.inputs.length > 0) {
      const batchSize = 100;
      const batches = [];

      // Split into batches of size 100
      for (let i = 0; i < batchInput.inputs.length; i += batchSize) {
        const batch = batchInput.inputs.slice(i, i + batchSize);
        batches.push(batch);
      }

      for (const batch of batches) {
        await createCompaniesInBatches({ inputs: batch });
      }
    }

    const batchInputForCustomObject = {
      inputs: s3CsvArray.map((item) => ({
        properties: cleanRecord({ ...item, order_id: item.order_id || -1 }),
        associations: [
          {
            to: {
              id: findRecordId(companyArray, item.order_code, item.order_id),
            },
            types: [
              {
                associationCategory: "USER_DEFINED",
                associationTypeId: "19",
              },
            ],
          },
        ],
      })),
    };
    await createBrandsInObjectInBatches(
      CUSTOM_OBJECT_ID,
      batchInputForCustomObject
    );

    console.log("Items created successfully");
  } catch (error) {
    console.error("Error during test:", error);
  }
})();
