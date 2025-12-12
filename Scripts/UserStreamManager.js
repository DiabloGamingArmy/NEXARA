import {
    initializeApp,
    getApps
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js';
import { getFunctions, httpsCallable } from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-functions.js';

const firebaseConfig = {
    apiKey: 'AIzaSyDg9Duz3xicI3pvvOtLCrV1DJRWDI0NtYA',
    authDomain: 'spike-streaming-service.firebaseapp.com',
    projectId: 'spike-streaming-service',
    storageBucket: 'spike-streaming-service.firebasestorage.app',
    messagingSenderId: '592955741032',
    appId: '1:592955741032:web:dbd629cc957b67fc69bcdd',
    measurementId: 'G-BF3GFFY3D6'
};

function getFirebase() {
    if (!getApps().length) {
        initializeApp(firebaseConfig);
    }
    const app = getApps()[0];
    return {
        functions: getFunctions(app)
    };
}

export class UserStreamManager {
    constructor() {
        const { functions } = getFirebase();
        this.functions = functions;
        this.initializeUserChannel = httpsCallable(this.functions, 'initializeUserChannel');
        this.createEphemeralChannel = httpsCallable(this.functions, 'createEphemeralChannel');
    }

    async ensurePersistentChannel(metadata = {}) {
        const response = await this.initializeUserChannel(metadata);
        return response?.data || {};
    }

    async createEphemeral(metadata = {}) {
        const response = await this.createEphemeralChannel(metadata);
        return response?.data || {};
    }
}

export function initialize() {}
export function teardown() {}
