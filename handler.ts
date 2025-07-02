import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import {
  DynamoDBClient,
  BatchWriteItemCommand,
} from "@aws-sdk/client-dynamodb";
import { Readable } from "stream";
import * as XLSX from "xlsx";
import { S3Handler } from "aws-lambda";

const s3 = new S3Client({});
const dynamoDB = new DynamoDBClient({});
const TABLE_NAME = "userTable";
const BATCH_SIZE = 25;

const streamToBuffer = async (stream: Readable): Promise<Buffer> => {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
};

const isValidEmail = (email: string): boolean =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

const isValidUrl = (url: string): boolean => /^(https?:\/\/[^\s]+)$/.test(url);

const isRowEmpty = (row: Record<string, any>): boolean =>
  !row ||
  Object.values(row).every(
    (val) => val === null || val === undefined || val === ""
  );

interface UserRow {
  userId: string | number;
  name: string;
  email: string;
  profileImageUrl?: string;
}

const validateUser = (
  user: any,
  index: number
): { valid: boolean; error?: string } => {
  if (isRowEmpty(user))
    return { valid: false, error: `Row ${index + 2}: Empty row` };
  if (
    !user.userId ||
    (typeof user.userId !== "string" && typeof user.userId !== "number")
  )
    return {
      valid: false,
      error: `Row ${index + 2}: Missing or invalid 'userId'`,
    };
  if (!user.name || typeof user.name !== "string")
    return {
      valid: false,
      error: `Row ${index + 2}: Missing or invalid 'name'`,
    };
  if (
    !user.email ||
    typeof user.email !== "string" ||
    !isValidEmail(user.email)
  )
    return { valid: false, error: `Row ${index + 2}: Invalid 'email'` };
  if (user.profileImageUrl && !isValidUrl(user.profileImageUrl))
    return {
      valid: false,
      error: `Row ${index + 2}: Invalid 'profileImageUrl'`,
    };
  return { valid: true };
};

const buildPutRequests = (validUsers: UserRow[]) =>
  validUsers.map((user) => ({
    PutRequest: {
      Item: {
        userId: { S: String(user.userId) },
        name: { S: user.name },
        email: { S: user.email },
        profileImage: { S: user.profileImageUrl ?? "" },
      },
    },
  }));

const writeToDynamoDBInBatches = async (putRequests: any[]) => {
  const batches = putRequests.reduce((acc: any[][], item, index) => {
    const chunkIndex = Math.floor(index / BATCH_SIZE);
    if (!acc[chunkIndex]) acc[chunkIndex] = [];
    acc[chunkIndex].push(item);
    return acc;
  }, []);

  for (const batch of batches) {
    await dynamoDB.send(
      new BatchWriteItemCommand({
        RequestItems: { [TABLE_NAME]: batch },
      })
    );
  }
};

export const handler: S3Handler = async (event) => {
  try {
    const record = event.Records[0];
    if (!record?.s3?.bucket?.name || !record?.s3?.object?.key) {
      console.error("Invalid event format");
      return;
    }
    const bucket = record?.s3?.bucket?.name;
    const key = decodeURIComponent(record?.s3?.object?.key.replace(/\+/g, " "));

    const s3Object = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    );
    const buffer = await streamToBuffer(s3Object.Body as Readable);

    const workbook = XLSX.read(buffer, { type: "buffer" });

    //Validate workbook and worksheet
    if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
      console.error("No sheets found in Excel file.");
      return;
    }

    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const jsonData = XLSX.utils.sheet_to_json(sheet, {
      header: ["userId", "name", "email", "profileImageUrl"],
      range: 1,
    });

    if (!jsonData || jsonData.length === 0) {
      console.error("Excel sheet is empty.");
      return;
    }

    const errors: string[] = [];
    const validUsers = (jsonData as UserRow[]).filter((user, i) => {
      const result = validateUser(user, i);
      if (!result.valid) errors.push(result.error!);
      return result.valid;
    });

    if (!validUsers.length) {
      console.warn("No valid records found.");
      return;
    }

    const putRequests = buildPutRequests(validUsers);
    await writeToDynamoDBInBatches(putRequests);

    console.log(`Uploaded ${putRequests.length} records to DynamoDB.`);
    if (errors.length) console.warn(`skipped ${errors.length} rows:`, errors);
  } catch (err) {
    console.error("Lambda failed:", err);
  }
};
