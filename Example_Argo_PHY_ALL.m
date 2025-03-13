%% ===================================================================== %%
% Temperature map - Argo PHY 'ALL'
% ======================================================================= %
%
% This example shows how to read and manipulate Argo data stored in parquet
% format. We will filter the data by pressure, time, and data quality. We 
% will then compute the average value of the adjusted temperature reported 
% by each float and plot it.
%
%% Setup
% Set up the reader. Here we generate a ParquetDatastore object of the 
% database (no need to know the details of what a ParquetDatastore object 
% exactly is).
% To read only some variables, create a `selectVariables` array with the 
% Argo parameters to read. Note that the dataset must load all the 
% variables to which filters are later applied.
% To read all the variables, just do not specify "SelectedVariableNames"
% when calling parquetDatastore().
%
%% Download
% If you have not downloaded the database yet, just run first:
% download_database("ARGO","PHY",false);

parquetPath = fullfile("./data/1011_PHY_ARGO-CLOUD-MATLAB/");
location = matlab.io.datastore.FileSet(parquetPath); % for faster parsing
selectVariables = [...
    "PLATFORM_NUMBER",...
    'LATITUDE',...
    "LONGITUDE",...
    "JULD",...
    "PRES",...
    "TEMP_ADJUSTED",...
    "TEMP_ADJUSTED_QC"
    ];
pds = parquetDatastore(...
            location, ...
            "FileExtensions",".parquet", ...
            "IncludeSubfolders", false, ...
            "OutputType", "table", ...    
            "VariableNamingRule","preserve", ...
            "SelectedVariableNames", selectVariables ...
        );

%% Filtering
% We now create the filters for our data. 
% We need first to create a RowFilter object from the dataset, then we
% populate it with the filters, and finally we will assign it back to the
% dataset.

% Generating RowFilter object
rf = rowfilter(pds);

% Creating filters on pressure, data quality and time. 'and' and 'or'
% operators are the bitwise operators '&' and '|' as usual in MATLAB.
% The supported relational operators are: <, <=, >, >=, ==, and ~= .
filter_pres_min = rf.("PRES") >= 900;
filter_pres_max = rf.("PRES") <= 1000;
filter_temp_qc = rf.("TEMP_ADJUSTED_QC") == 1;

startTime = datetime(2023,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
endTime   = datetime(2024,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
filter_time = rf.("JULD") >= startTime & rf.("JULD") <= endTime ;

% Combining the two filters and assigning it to the ParquetDataset
% object
filter = filter_pres_min & filter_pres_max & filter_time & filter_temp_qc;
pds.RowFilter = filter;

%% Reading the data into memory, in parallel (timing the operation)
% uncomment the following lines to read data in parallel (slightly faster)
p = gcp("nocreate");
if isempty(p)
    tic
    parpool; % you can specificy the number of workers with 
             % parpool(nbWorkers); the default shoud be fine
    elapsed = toc;
    disp("Elapsed time to  create parallel environment: " + num2str(elapsed) + " seconds.")
end
tic;
dataPHY = readall(pds,UseParallel=true);
elapsed = toc;
disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")

%% Reading the data into memory, serially (timing the operation)
% uncomment the following lines to read data serially (slower)
% tic;
% dataPHY = readall(pds,UseParallel=false);
% elapsed = toc;
% disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

%% Plotting target data
% Now we can make a scatter plot of the dissolved oxygen data recorded
varName = 'TEMP_ADJUSTED';

% check that (lat0,lon0) are unique, otherwise average data
[G, LAT, LON] = findgroups(dataPHY.LATITUDE,dataPHY.LONGITUDE);
if height(dataPHY) ~= height(G)
    meanVar = splitapply(@mean, dataPHY.(varName), G);
    refTable = table( ...
        LAT, LON, meanVar, ...
        'VariableNames', {'LATITUDE', 'LONGITUDE', varName} ...
        );
else
    refTable = dataPHY;
end

% setup figure
f = figure("Position", [100 300 900 800]) ;
gx = geoaxes( ...
    'Basemap','None', ...
    'Grid','on' ...
    );
geobasemap('satellite');
geolimits( [-90,90], [-180,180] );
rowsToKeep = refTable.LATITUDE <= 90 & refTable.LATITUDE >= -90;
refTable = refTable(rowsToKeep, :);
rowsToKeep = refTable.LONGITUDE <= 180 & refTable.LONGITUDE >= -180;
refTable = refTable(rowsToKeep, :);
geoscatter(...
        refTable.LATITUDE, ...
        refTable.LONGITUDE, ...
        20, ...
        refTable.(varName), ...
        'filled' ...
        );
colormap("copper")
colorbar
title("Temperature adjusted measurements");

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataPHY);
% and for example see the min, max, and median values of the temperatue
disp("Minimum temperature = " + num2str(dataStats.TEMP_ADJUSTED.Min) + " degrees");
disp("Maximum temperature = " + num2str(dataStats.TEMP_ADJUSTED.Max) + " degrees");
disp("Median temperature = " + num2str(dataStats.TEMP_ADJUSTED.Median) + " degrees");
