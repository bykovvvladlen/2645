const fs = require('fs');
const AParser = require('a-parser-client');
const axios = require('axios');
const config = {};

(async function() {
    try {
        await getConfig();
    }

    catch(error) {
        console.log(error);
        return;
    }

    console.log(config);
    try {
        console.log(await connect());
    }

    catch(error) {
        console.log(error);
        return;
    }

    const presets = [
        {
            queriesFilename: 'auto-ru',
            parser: 'JS::Order::2571',
        }, {
            queriesFilename: 'avito',
            parser: 'JS::Order::2564',
        },
    ];

    for (let { queriesFilename, parser } of presets) {
        (async function() {
            while (true) {
                queries = await getQueries(queriesFilename);
                const taskUid = await addTask(parser, 'Order::2645', queries, config.exclusions[queriesFilename], queriesFilename).catch(error => console.log(error));
                if (!taskUid) {
                    console.log(`[${queriesFilename}] Wait 10 seconds`);
                    await sleep(10000);
                    continue;
                }
            
                console.log(`[${queriesFilename}] Wait for task #${taskUid}`);
                
                try {
                    await waitForTask(taskUid);
                }

                catch(error) {
                    console.log(`[${queriesFilename}] ${error}`);
                    continue;
                }
            
                console.log(`[${queriesFilename}] Task #${taskUid} completed`);
                console.log(`[${queriesFilename}] Getting results file`);
                let result;
                
                try {
                    result = await getResults(taskUid);
                }
            
                catch(error) {
                    console.log(`[${queriesFilename}] ${error}`);
                    continue;
                }
            
                await extractExclusions(result, queriesFilename).catch(error => console.log(`[${queriesFilename}] ${error}`));
                await sendResults(result).catch(error => console.log(`[${queriesFilename}] ${error}`));
            }
        })();
    }
})();

function sendResults(data) {
    return new Promise(async (resolve, reject) => {
        const remote = config.remote;

        if (!remote) {
            reject(`Remote url not found`);
        }

        let response;

        try {
            response = await axios.post(remote, data, {
                headers: {
                    'Content-Type': 'application/json',
                }
            });
        }

        catch(error) {
            reject(`Can't send results to ${remote}: ${error}`);
        }

        resolve();
    });
}

function extractExclusions(json, preset) {
    return new Promise((resolve, reject) => {
        let object;

        try {
            object = JSON.parse(json); 
        }

        catch(error) {
            reject(`Can't parse results ${error}`);
        }

        let counter = {};
        object.forEach(({ id, region }) => {
            if (!counter[region]) counter[region] = 0;

            if (!config.exclusions[preset][region]) {
                config.exclusions[preset][region] = [];
            }

            if (!config.exclusions[preset][region].includes(id)) {
                config.exclusions[preset][region].push(id);
                counter[region]++;
            }
        });

        for (let region in counter) {
            const count = counter[region];
            console.log(`[${preset}|${region}] Found ${count} new items`);
        }
        
        resolve();
    });
}

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(() => resolve(), ms);
    });
}

function getResults(taskUid) {
    return new Promise(async (resolve, reject) => {
        let response;

        try {
            response = await config.aparser.makeRequest('getTaskResultsFile', { taskUid });
        }

        catch(error) {
            reject(`Can't get results file ${error}`);
        }

        const url = response?.data;
        const result = await axios.get(url);
        if (Array.isArray(result?.data) && result?.data?.length === 0) {
            reject('No results');
            return;
        }

        let formattedResult;

        try {
            formattedResult = result?.data?.replace(/,[\s\n\r]*\]$/, ']');
        }

        catch(error) {
            console.log(result);
            reject(`Can't get results file ${error}`);
        }

        resolve(formattedResult);
    });
}

function waitForTask(taskUid) {
    return new Promise(async (resolve, reject) => {
        let status;
        while (status != 'completed' && status != 'stopped' && status != 'paused') {
            if (status != undefined) await sleep(5000);
            const response = await config.aparser.makeRequest('getTaskState', { taskUid });
            status = response?.data?.status;
        }

        if (['stopped', 'paused'].includes(status)) {
            reject(`Task #${taskUid} stopped`);
        }

        else {
            resolve();
        }
    });
}

function addTask(parser, preset, queries, exclusions, queriesFilename) {
    return new Promise(async (resolve, reject) => {
        const response = await config.aparser.makeRequest('addTask', {
            configPreset: 'default',
            parsers: [[ 
                parser, 
                preset, {
                    type: 'override',
                    id: 'proxyretries',
                    value: '20',
                }, {
                    type: 'override',
                    id: 'duplicate',
                    value: JSON.stringify(exclusions),
                },
            ]],
            resultsFormat: '$p1.preset',
            resultsSaveTo: 'file',
            resultsFileName: 'order2645/' + queriesFilename + '/$datefile.format().json',
            additionalFormats: [],
            resultsUnique: 'no',
            queriesFrom: 'text',
            queryFormat: ['$query'],
            uniqueQueries: false,
            saveFailedQueries: false,
            iteratorOptions: {
                onAllLevels: false,
                queryBuildersAfterIterator: false,
                queryBuildersOnAllLevels: false,
            },
            resultsOptions: {
                overwrite: false,
                writeBOM: false,
            },
            // doLog: 'db',
            limitLogsCount: 0,
            keepUnique: 'No',
            moreOptions: true,
            resultsPrepend: '[',
            resultsAppend: ']',
            queryBuilders: [],
            resultsBuilders: [],
            configOverrides: [],
            runTaskOnComplete: null,
            useResultsFileAsQueriesFile: false,
            runTaskOnCompleteConfig: 'default',
            toolsJS: '',
            prio: 5,
            removeOnComplete: false,
            callURLOnComplete: '',
            queries,
        });

        if (response?.success) {
            resolve(response?.data);
        }

        else {
            reject(`Can't add task ${JSON.stringify(response)}`);
        }
    });
}

function connect() {
    return new Promise(async (resolve, reject) => {
        const aparser = new AParser(config.url, config.pass);
        let response;

        try {
            response = await aparser.ping();
        }

        catch(error) {
            reject(`Can't connect to A-Parser: ${error}`);
        }

        if (response.data === 'pong') {
            config.aparser = aparser;
            resolve('A-Parser connected');
        }

        else {
            reject(`Something went wrong with A-Parser: ${JSON.stringify(response)}`);
        }
    });
}

function getConfig() {
    return new Promise((resolve, reject) => {
        if (fs.existsSync('./config/aparser.txt')) {
            const file = fs.readFileSync('./config/aparser.txt', 'utf-8');
            const params = [...file.matchAll(/^(\w+):\s*(.+?)$/gm)].reduce((acc, item) => {
                const key = item[1];
                const value = item[2].trim();
                acc[key] = value;
                return acc;
            }, {});
        
            config.url = params.url;
            config.pass = params.pass;
            config.remote = params.remote;
            config.exclusions = {
                'auto-ru': {},
                'avito': {},
            };

            resolve();
        }

        else reject(`Can't find ./config/aparser.txt`);
    });
}

function getQueries(parser) {
    return new Promise((resolve, reject) => {
        const path = `./config/${parser}.txt`;
        if (fs.existsSync(path)) {
            const file = fs.readFileSync(path, 'utf-8');
            const queries = file.split(/[\r\n]+/g).filter(item => item).map(val => val.trim());
            resolve(queries);
        }

        else reject(`Can't find ${path}`);
    });
}