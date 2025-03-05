%% ===================================================================== %%
% Loading BGC coverage
% ======================================================================= %
%
% This example shows how to read and manipulate Argo data stored in parquet
% format. We will filter the data by depth (pressure) and time. We will 
% then compute the average value of the chlorophyll measured by each float 
% and plot it.
%
% This script has been developed and tested on MATLAB R2024a. MATLAB R2022a
% and newer versions should be supported. Older versions will almost 
% certainly fail -- parquet is a fairly recent format and MATLAB support is
% even more recent.
%
% 2025-01-08 Update:
% In the new version of the QC-ed database, only the best values for each
% paramater are kept and only one parameter name <PARAM> (and <PARAM>_QC)
% are in the database. <PARAM> contains the value of the GDAC's
% <PARAM>_ADJUSTED whenever this is present and <PARAM>_ADJUSTED_QC in
% [1,2], otherwise the value of <PARAM> if <PARAM>_QC in [1,2], otherwise
% NaN.
%
%% Setup
% Set up the reader. Here we generate a ParquetDatastore object of the 
% database (no need to know the details of what a ParquetDatastore object 
% exactly is).
% To read only some variables, create a `selectVariables` array with the 
% Argo parameters to read.
% Note that the dataset must load all the variables to which filters are 
% later applied (this is not always the case in python).
% To read all the variables, just do not specify "SelectedVariableNames"
% when calling parquetDatastore()

%% NB
% If you have not downloaded the database yet, just run first:
% download_database("CROCOLAKE","BGC",true);

parquetPath = fullfile("./data/0006_BGC_CROCOLAKE-QC-MERGED-DEV-MATLAB/");
location = matlab.io.datastore.FileSet(parquetPath); % for faster parsing
selectVariables = [...
    "DB_NAME",...
    "LATITUDE",...
    "LONGITUDE",...
    "JULD",...
    "PRES",...
    "TEMP",...
    "DOXY",...
    "ABS_SAL_COMPUTED",...
    "CONSERVATIVE_TEMP_COMPUTED",...
    "SIGMA1_COMPUTED"
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
filter_pres = rf.("PRES") <= 20;
filter_lat = rf.("LATITUDE") <= 60 & rf.("LATITUDE") >= 0 ;
filter_lon = rf.("LONGITUDE") <= 0 & rf.("LONGITUDE") >= -90 ;

startTime = datetime(2010,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
endTime   = datetime(2022,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
filter_time = rf.("JULD") >= startTime & rf.("JULD") <= endTime ;

% Combining the two filers in one and assigning it to the ParquetDataset
% object
filter = filter_pres & filter_time & filter_lat & filter_lon;
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
dataBGC = readall(pds,UseParallel=true);
elapsed = toc;
disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")

%% Reading the data into memory, serially (timing the operation)
% uncomment the following lines to read data serially (slower)
% tic;
% dataBGC = readall(pds,UseParallel=false);
% elapsed = toc;
% disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

%% Plotting target data
% Now we can make a scatter plot of the dissolved oxygen data recorded
varName = 'DOXY';
% check that (lat0,lon0) are unique, otherwise average data
[G, LAT, LON] = findgroups(dataBGC.LATITUDE,dataBGC.LONGITUDE);
if height(dataBGC) ~= height(G)
    meanVar = splitapply(@mean, dataBGC.(varName), G);
    refTable = table( ...
        LAT, LON, meanVar, ...
        'VariableNames', {'LATITUDE', 'LONGITUDE', varName} ...
        );
else
    refTable = dataBGC;
end
percentiles = prctile(refTable.(varName), [10, 90]);

f = figure("Position", [100 300 900 800]) ;
gx = geoaxes( ...
    'Basemap','None', ...
    'Grid','on' ...
    );
geobasemap('satellite');
for db_name = ["ARGO","GLODAP"]
    if db_name=="ARGO"
        mrk = 'o';
        sz = 10;
    else
        mrk = 'o';
        sz = 10;
    end

    plotTable = refTable(strcmp(refTable.DB_NAME, db_name), :);
    geoscatter(...
            plotTable.LATITUDE, ...
            plotTable.LONGITUDE, ...
            sz, ...
            'filled',...
            'Marker',...
            mrk...
            );

    colormap(gx,"copper");
    clim(gx, percentiles);
    hold on;
end
geolimits( [0,60], [-90,0] );
title("Dissolved oxygen measurements");

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataBGC);
% and for example see the min, max, and median values of the temperatue
disp("Minimum dissolved oxygen = " + num2str(dataStats.DOXY.Min) + " micromole/kg");
disp("Maximum dissolved oxygen = " + num2str(dataStats.DOXY.Max) + " micromole/kg");
disp("Median dissolved oxygen = " + num2str(dataStats.DOXY.Median) + " micromole/kg");
