import { Request, Response } from "express";
import { handleResponseError } from "../route-handlers/route-error-handler";
import { Collection, ObjectId } from "mongodb";
import {
  ISegment,
  ISegmentGenderData,
  ISegmentMetaData,
} from "../../common/types/db-models/segment";
import { IUser } from '../../common/types/db-models/user';
import { getDbWrapper } from "../../common/db/mongo-wrapper";


export async function segmentList(req: Request<{}, {}, {}, { limit: number, skip: number }>, res: Response): Promise<void> {
  try {
    const segmentCollection: Collection = await getDbWrapper().getCollection("segments");
    const limit = req.query.limit || 10;
    const skip = req.query.skip || 0;

    const segments = await segmentCollection.find().skip(skip).limit(limit).toArray();
    const segmentIds = segments.map(s => s._id);

    // todo TASK 1
    // write this function to return { data: ISegmentMetaData[]; totalCount: number };
    // where data is an array of ISegmentMetaData, and totalCount is the # of total segments

    // the "users" collection
    const userCollection: Collection = await getDbWrapper().getCollection('users');
    // has a "many to one" relationship to the segment collection, check IUser interface or query the raw data.

    const cursor = userCollection.aggregate(
      // Limit is a hack to make the solution work
      // TODO: As possible solution, create additional collection with aggregated user data and update it in some interval
      [ { $limit: 100000 },
        { $match: { segment_ids: { $in: segmentIds }} },
        { $project: {
           _id: 0,
           segment_ids: 1,
           gender: 1,
           income_level: { 
              $cond: {
                  if: {
                    $eq : ['$income_type', 'yearly'],
                  },
                  then: { $divide: ['$income_level', 12] },
                  else: '$income_level',
              },
            },
        }
      },
      { $unwind: {
          path: '$segment_ids' } },
        { $match: { segment_ids: { $in:  segmentIds }} },
        { $group: {
          _id: '$segment_ids',
          userCount: { $sum: 1 },
          avgIncome: { $avg: '$income_level' },
          Male: { $sum: { $cond :  [{ $eq : ["$gender", "Male"]}, 1, 0]} },
          Female: { $sum: { $cond :  [{ $eq : ["$gender", "Female"]}, 1, 0]} },
          Other: { $sum: { $cond :  [{ $eq : ["$gender", "Other"]}, 1, 0]} },
        } }
      ],
      { allowDiskUse: true }
    );

    const segmentUserMap = {};

    for await (const doc of cursor) {
      segmentUserMap[doc._id] = {
        userCount: doc.userCount,
        avgIncome: Math.round(doc.avgIncome),
        topGender: ['Male', 'Female', 'Other'].reduce((a, b) => doc[a] > doc[b] ? a : b)
      };
  }

const total = await segmentCollection.estimatedDocumentCount({});

  res.json({
     success: true,
     data: segments.map(s => ({
      ...s,
      ...segmentUserMap[s._id],
     })),
    totalCounter: total
  });
  
  } catch (error) {
    handleResponseError(
      `Get Segment List Error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");
    const segment: ISegment = await segmentCollection.findOne({
      _id: new ObjectId(req.params.id as string),
    });
    if (!segment) {
      return handleResponseError(
        `Error getSegmentById`,
        `Segment with id ${req.params.id} not found.`,
        res
      );
    }
    res.json({ success: true, data: segment });
  } catch (error) {
    handleResponseError(
      `Get Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function updateSegmentById(
  req: Request,
  res: Response
): Promise<void> {
  try {
    // res.json({ success: true });
  } catch (error) {
    handleResponseError(
      `Update Segment by id error: ${error.message}`,
      error.message,
      res
    );
  }
}

export async function getSegmentGenderData(
  req: Request,
  res: Response
): Promise<void> {
  try {
    const segmentCollection: Collection = await (
      await getDbWrapper()
    ).getCollection("segments");

    // todo TASK 2
    // write this function to return
    // data = [ { _id: "Male", userCount: x1, userPercentage: y1 }, { _id: "Female", userCount: x2, userPercentage: y2} ]

    // the "users" collection
    const userCollection: Collection = await (await getDbWrapper()).getCollection('users');
    // has a "many to one" relationship to the segment collection, check IUser interface or query the raw data.
    // res.json({ success: true, data: ISegmentGenderData[] });
  
    const id = new ObjectId(req.params.id as string);

    const cursor = userCollection.aggregate(
      [ { $limit: 100000 }, //the same hack as above
        { $match: { segment_ids: { $elemMatch: { $eq: id } }} },
        { $project: { _id:0, gender: 1 }},
        { $group: {
          _id: '$gender',
          userCount: { $sum: 1 }
        } 
      },

      ],
      { allowDiskUse: true });

      const aggregationData = await cursor.toArray();
      const total = aggregationData.reduce((accumulator, value) => accumulator + value.userCount, 0);

    res.json({
      success: true,
      data:  aggregationData.map(gender => {
      return {
        ...gender,
        userPercentage: Math.round((gender.userCount / total) * 100)
      }
    })});
  } catch (error) {
    handleResponseError(
      `Segment gender data error: ${error.message}`,
      error.message,
      res
    );
  }
}
