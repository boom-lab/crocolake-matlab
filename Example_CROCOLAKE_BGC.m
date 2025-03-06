%% ===================================================================== %%
% nitrate map - CrocoLake BGC
% ======================================================================= %
%
% This example shows how to read and manipulate CrocoLake data.
% We will filter the data by pressure, time, and location. For each dataset 
% (Argo, GLODAP), we will then plot each location containing a measurement
% of the nitrate.
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
    "NITRATE",...
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
% Now we can make a scatter plot of the nitrate data recorded
varName = 'NITRATE';
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
handles = cell(1, 2);
count = 0;
db_names = ["ARGO","GLODAP"];
for db_name = db_names
    count = count + 1;
    if db_name=="ARGO"
        colour = [0.8500 0.3250 0.0980];
    elseif db_name=="GLODAP"
        colour = [0.4660 0.6740 0.1880];
    else
        colour = [0.9290 0.6940 0.1250];
    end

    plotTable = refTable(strcmp(refTable.DB_NAME, db_name), :);
    handles{count} = geoscatter(...
            plotTable.LATITUDE, ...
            plotTable.LONGITUDE, ...
            10, ...
            'filled',...
            'Marker','o',...
            'MarkerFaceColor', colour...
            );

    colormap(gx,"copper");
    clim(gx, percentiles);
    hold on;

    % statistics
    dataStats = summary(plotTable);
    disp("Minimum nitrate in " + db_name + " dataset: " + num2str(dataStats.NITRATE.Min) + " micromole/kg");
    disp("Maximum nitrate in " + db_name + " dataset: " + num2str(dataStats.NITRATE.Max) + " micromole/kg");
    disp("Median nitrate in " + db_name + " dataset: " + num2str(dataStats.NITRATE.Median) + " micromole/kg");
end
% geolimits( [0,60], [-90,0] );
legend([handles{:}], db_names);
title("Nitrate measurements in [0,60], [-90,0]");

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataBGC);
% and for example see the min, max, and median values of the temperatue
disp("Minimum nitrate = " + num2str(dataStats.NITRATE.Min) + " micromole/kg");
disp("Maximum nitrate = " + num2str(dataStats.NITRATE.Max) + " micromole/kg");
disp("Median nitrate = " + num2str(dataStats.NITRATE.Median) + " micromole/kg");
