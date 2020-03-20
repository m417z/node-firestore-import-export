import * as admin from 'firebase-admin';
import * as loadJsonFile from 'load-json-file';
import {IFirebaseCredentials} from '../interfaces/IFirebaseCredentials';


const getCredentialsFromFile = (credentialsFilename: string): Promise<IFirebaseCredentials> => {
  return loadJsonFile(credentialsFilename);
};

const getFirestoreDBReference = (credentials: IFirebaseCredentials): admin.firestore.Firestore => {
  admin.initializeApp({
    credential: admin.credential.cert(credentials as any),
    databaseURL: `https://${(credentials as any).project_id}.firebaseio.com`,
  });

  return admin.firestore();
};

const getDBReferenceFromPath = (db: admin.firestore.Firestore, dataPath?: string): admin.firestore.Firestore |
  FirebaseFirestore.DocumentReference |
  FirebaseFirestore.CollectionReference => {
  let startingRef;
  if (dataPath) {
    const parts = dataPath.split('/').length;
    const isDoc = parts % 2 === 0;
    startingRef = isDoc ? db.doc(dataPath) : db.collection(dataPath);
  } else {
    startingRef = db;
  }
  return startingRef;
};

const isLikeDocument = (ref: admin.firestore.Firestore |
  FirebaseFirestore.DocumentReference |
  FirebaseFirestore.CollectionReference): ref is FirebaseFirestore.DocumentReference => {
  return (<FirebaseFirestore.DocumentReference>ref).collection !== undefined;
};

const isRootOfDatabase = (ref: admin.firestore.Firestore |
  FirebaseFirestore.DocumentReference |
  FirebaseFirestore.CollectionReference): ref is admin.firestore.Firestore => {
  return (<admin.firestore.Firestore>ref).batch !== undefined;
};

const sleep = (timeInMS: number): Promise<void> => new Promise(resolve => setTimeout(resolve, timeInMS));

const batchExecutor = async function<T>(promiseGenerators: (() => Promise<T>)[], batchSize: number = 50) {
  const res: T[] = [];
  while (promiseGenerators.length > 0) {
    const promises = promiseGenerators.splice(0, batchSize).map(generator => generator());
    res.push(...await Promise.all(promises))
  }
  return res;
};

export {
  getCredentialsFromFile,
  getFirestoreDBReference,
  getDBReferenceFromPath,
  isLikeDocument,
  isRootOfDatabase,
  sleep,
  batchExecutor
};
