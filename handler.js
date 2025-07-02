import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import {
  DynamoDBClient,
  BatchWriteItemCommand,
} from "@aws-sdk/client-dynamodb";
import { Readable } from "stream";
import XLSX from "xlsx";

const s3 = new S3Client({});
const dynamoDB = new DynamoDBClient({});
const TABLE_NAME = "userTable";
const BATCH_SIZE = 25;

const streamToBuffer = async (stream) => {
  const chunks = [];
  for await (const chunk of stream) {
    chunks?.push(chunk);
  }
  return Buffer.concat(chunks);
};

const isValidEmail = (email) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

const isValidUrl = (url) =>
  /^(https?:\/\/[^\s]+)$/.test(url);

const isRowEmpty = (row)=>
  !row || Object.values(row).every((val) => val === null || val === undefined || val === "");

const validateUser = (user, index) => {
  if (isRowEmpty(user)) {
    return { valid: false, error: `Row ${index + 2}: Empty row` };
  }
  if (!user?.userId || (typeof user?.userId !== "string" && typeof user?.userId !== "number")) {
    return { valid: false, error: `Row ${index + 2}: Missing or invalid 'userId'` };
  }
  if (!user?.name || typeof user?.name !== "string") {
    return { valid: false, error: `Row ${index + 2}: Missing or invalid 'name'` };
  }
  if (!user?.email || typeof user?.email !== "string" || !isValidEmail(user.email)) {
    return { valid: false, error: `Row ${index + 2}: Invalid 'email'` };
  }
  if (user.profileImageUrl && !isValidUrl(user.profileImageUrl)) {
    return { valid: false, error: `Row ${index + 2}: Invalid 'profileImageUrl'` };
  }
  return { valid: true };
};

const buildPutRequests = (validUsers) =>
  validUsers.map((user) => ({
    PutRequest: {
      Item: {
        userId: { S: user?.userId.toString() },
        name: { S: user?.name },
        email: { S: user?.email },
        profileImage: { S: user?.profileImageUrl ?? "" },
      },
    },
  }));

const writeToDynamoDBInBatches = async (putRequests) => {
  const batches = putRequests.reduce((acc, item, index) => {
    const chunkIndex = Math.floor(index / BATCH_SIZE);
    if (!acc[chunkIndex]) acc[chunkIndex] = [];
    acc[chunkIndex].push(item);
    return acc;
  }, []);

  for (const batch of batches) {
    await dynamoDB.send(new BatchWriteItemCommand({
      RequestItems: { [TABLE_NAME]: batch },
    }));
  }
};;

export const handler = async (event) => {
  try {
    const record = event.Records?.[0];
    if (!record?.s3?.bucket?.name || !record?.s3?.object?.key) {
      console.error("Invalid event format");
      return;
    }

    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
    const s3Object = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const buffer = await streamToBuffer(s3Object.Body);

    const workbook = XLSX.read(buffer, { type: "buffer" });

    //Validate workbook and worksheet
    if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
      console.error("No sheets found in Excel file.");
      return;
    }

    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const jsonData = XLSX.utils.sheet_to_json(sheet);

    if (!jsonData || jsonData.length === 0) {
      console.error("Excel sheet is empty.");
      return;
    }

    const errors=[];
    const validUsers = jsonData.filter((user, index) => {
      const result = validateUser(user, index);
      if (!result.valid) errors.push(result.error);
      return result.valid;
    });

    if (!validUsers.length) {
      console.warn("No valid records found.");
      return;
    }

    const putRequests = buildPutRequests(validUsers);
    await writeToDynamoDBInBatches(putRequests);

    console.log(`Uploaded ${putRequests.length} records to DynamoDB.`);
    if (errors.length) {
      console.log(`skipped ${errors.length} rows due to validation errors.`);
    }
  } catch (err) {
    console.error("Lambda failed:", err);
  }
};
