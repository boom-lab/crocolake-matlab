function download_database(db_name, db_type, db_qc, rm_zip)

%% Donwload specific database to disk
%
% Example usage: download_database("ARGO","BGC",true);
%
% db_name: one of "ARGO" or "CROCOLAKE". 
%          ARGO downloads Argo data only (you can think of it as a parquet 
%          version of the Argo GDAC).
%          CROCOLAKE downloads CrocoLake, which is a parquet database with
%          observations from multiple sources. It currently includes Argo,
%          GLODAP and Spray Gliders data. 
% db_type: one of "PHY" or "BGC". PHY downloads CTD data only. BGC
%          downloads all parameters in the Argo BGC float.
% db_qc  : If True, it downloads QCed observations only. For example, for 
%          Argo it downloads quality-controlled data only in a unique 
%          parameter variable per parameter (i.e. no <PARAM>_ADJUSTED 
%          variable exists, but <PARAM> contains adjusted values if their 
%          QC flag is good enough. See [url] for more info.
%          If False, it downloads a larger set of observations of mixed 
%          quality. For example, for Argo it includes both adjusted and 
%          non-adjusted parameters. Note that CrocoLake only accepts
%          db_qc=true.
% rm_zip:  Flag to remove (if true, default) or keep (if false) the zip
%          archive after data extraction.

    if nargin < 4
        rm_zip = true;
    end

    % Get name and link
    db_codename = get_db_codename(db_name, db_type, db_qc);
    urls = get_urls();
    download_link = urls(db_codename);
    
    % Create data folder
    destination_folder = "./data/";
    mkdir(destination_folder);
    destination_name = destination_folder+db_codename+".zip";
    
    % Download db
    disp("Saving to " + destination_name + ". This might take a while...");
    websave(destination_name,download_link);
    disp("Saved!")

    % Unzip archive and remove zip file
    archive_folder = destination_folder+db_codename+"/";
    disp("Extracting archive to " + archive_folder + "...");
    unzip(destination_name, archive_folder);
    disp("Done.")
    if rm_zip
        disp("Removing .zip file...");
        delete(destination_name);
        disp("Done.")
    end

end

function db_codename = get_db_codename(db_name, db_type, qc)
    if strcmpi(db_name, 'CROCOLAKE')
        if ~qc
            error('CrocoLake database available only with QC.');
        end
        if strcmpi(db_type, 'PHY')
            db_codename = '0006_PHY_CROCOLAKE-QC-MERGED-DEV';
        elseif strcmpi(db_type, 'BGC')
            db_codename = '0006_BGC_CROCOLAKE-QC-MERGED-DEV';
        else
            error('Invalid database type. Must be ''PHY'' or ''BGC''.');
        end

    elseif strcmpi(db_name, 'ARGO')
        if strcmpi(db_type, 'PHY')
            if qc
                db_codename = '1002_PHY_ARGO-QC-DEV';
            else
                db_codename = '1010_PHY_ARGO-CLOUD-DEV';
            end
        elseif strcmpi(db_type, 'BGC')
            if qc
                db_codename = '1002_BGC_ARGO-QC-DEV';
            else
                db_codename = '1010_BGC_ARGO-CLOUD-DEV';
            end
        else
            error('Invalid database type. Must be ''PHY'' or ''BGC''.');
        end
    else
        error('Invalid database name. Must be ''CROCOLAKE'' or ''ARGO''.');
    end

    db_codename = db_codename + "-MATLAB";
end

function urls = get_urls()
    urls = containers.Map;
    urls('0006_PHY_CROCOLAKE-QC-MERGED-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EZ5RMKSI1pVLoLamkiW4Jv0BKQv7T4ql2PKFiVm5ERHjow?e=4mAN9T&download=1';
    urls('0006_BGC_CROCOLAKE-QC-MERGED-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EfrMqtCBPW9Gnwlqp0CC42oBCCx8UG88_6LfXcWbDLyiNQ?e=NUoQQd&download=1';
    urls('1002_PHY_ARGO-QC-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EbTstYpsb8RLhgkYig_HcZMBq26o4AV3DGUFVvJb00Hi5Q?e=0988Rb&download=1';
    urls('1002_BGC_ARGO-QC-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/ETB2Yiqffh1BrGobDrB8qvUBwJOcSmwWpGV52ui4hEtzbg?e=SX2JBE&download=1';
    urls('1010_PHY_ARGO-CLOUD-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EY7ioE0q_mlOidURqp8LtdYBGOjKmQWsejGWajnD4_zfuw?e=87YJot&download=1';
    urls('1010_BGC_ARGO-CLOUD-DEV-MATLAB') = 'https://whoi-my.sharepoint.com/:u:/g/personal/enrico_milanese_whoi_edu/EZiZQTiiyuZBsoG1yl96su0BGXkLCCi5PqdSUarG_k4Gxg?e=9t86Uz&download=1';
end