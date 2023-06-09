const fs = require('fs');
const probables = require('C:\\Users\\johnt\\dinger_dicter\\dinger_dicter\\helper_functions\\baseball-probable-pitchers\\probables.js');

const getData = async() => {
    try {

        // Today's date, or provide your own: format 2019-03-24
        // const d = new Date();
        const day = process.argv[2] || new Date();
        // const day = `${d.getFullYear()}-${d.getMonth()+1}-${d.getDate()}`;
        // const day = '2019-04-15';
        console.log(day);

        // File to write to
        const outputFile = 'C:\\Users\\johnt\\dinger_dicter\\dinger_dicter\\probable-pitchers.json';

        // Get pitchers
        probables.mlbpitchers.getPitchers(day, (data) => {
            fs.writeFile(outputFile, JSON.stringify(data), {flag: 'w'}, (err) => {
                if (err) {
                    console.error(`Error in writing to ${file}: ${err}`);
                } else {
                    console.error(`Probable pitchers successfully written to ${outputFile}.`);
                }
            });
        });
    } catch (err) {
        console.error(`Error in getData(): ${err}`);
    }

};

getData();

