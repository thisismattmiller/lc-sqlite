const fs = require('fs')
const glob = require('glob')
const H = require('highland')
const marc = require('marcjs')
// const sqlite3 = require('sqlite3').verbose();
var Database = require('better-sqlite3');
var db = new Database('lc_books.db');


// var db = new sqlite3.Database('lc_books.db');


var path = "/Volumes/Byeeee/lcmarc/books/*"
var count = 0


var stmt = db.prepare(`DROP TABLE IF EXISTS subjects`).run()
var stmt = db.prepare(`CREATE TABLE subjects(
          type TEXT,
          lccn TEXT,
          isbn TEXT,
          pub_date NUMBER,
          title TEXT,
          subtitle TEXT,
          label TEXT,
          numeration TEXT,
          sub_label TEXT,
          event_dates TEXT,
          date_of_work TEXT,
          location TEXT,
          form TEXT,
          general TEXT,
          chronological TEXT,
          geographic TEXT
        );`).run()

var stmt = db.prepare(`CREATE INDEX type_index on subjects(type);`).run()
var stmt = db.prepare(`CREATE INDEX lccn_index on subjects(lccn);`).run()
var stmt = db.prepare(`CREATE INDEX label_index on subjects(label);`).run()
var stmt = db.prepare(`CREATE INDEX form_index on subjects(form);`).run()
var stmt = db.prepare(`CREATE INDEX general_index on subjects(general);`).run()
var stmt = db.prepare(`CREATE INDEX chronological_index on subjects(chronological);`).run()
var stmt = db.prepare(`CREATE INDEX geographic_index on subjects(geographic);`).run()

 
glob(path, {}, function (er, files) {

    var processMarc = function (file, callback) {

      var insertIntoDB = (dataAry,cb) => {

        dataAry.forEach((data)=>{
          var stmt = db.prepare('INSERT INTO subjects (type, lccn, isbn, pub_date, title, subtitle, label, numeration, sub_label, event_dates, date_of_work, location, form, general, chronological, geographic) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)')
          stmt.run(data.type,data.lccn,data.isbn,data.date,data.title,data.subtitle,data.label,data.numeration,data.sub_label,data.event_dates,data.date_of_work,data.location,data.form,data.general,data.chronological,data.geographic)
        })

       
        cb(null,"yeah")

      }


      H(new marc.Iso2709Reader(fs.createReadStream(file)))
        .map((record) => {
          count++
          
          var mij = record.toMiJ()

          var r_base = {
            type: null,
            lccn: null, // removes spaces and '//XX notations'
            isbn: null, // removes comments like (pbk)
            date: null, // number
            title: null,
            subtitle: null,

            label: null,
            numeration: null,
            sub_label: null,
            event_dates: null, 
            date_of_work: null,
            location: null,
            form: null, 
            general: null,
            chronological: null,
            geographic: null
          }

          mij.fields.forEach((f)=>{

            if (f['010']){
              if (f['010'].subfields){              
                f['010'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r_base.lccn = sf.a.split('//')[0].replace(/\s/g,'')
                  }
                })
              }
            }
            if (f['020']){
              if (f['020'].subfields){
                f['020'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r_base.isbn = sf.a.replace(/\(/,' (').split(" ")[0].replace(/\:/g,'').split(" ")[0]
                    // if it looks weird then let it be weird
                    if (isNaN(r_base.isbn.replace(/x/gi,''))){
                     r_base.isbn = sf.a.trim()
                    }                    
                  }
                })
              }
            }           
            if (f['008']){
                if (!isNaN(f['008'].substring(7,11))){
                  r_base.date = parseInt(f['008'].substring(7,11))
                }
            } 
            if (f['245']){
              if (f['245'].subfields){
                f['245'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r_base.title = sf.a
                  }
                  if(sf.b){
                    r_base.subtitle = sf.b
                  }
                })
              }
            } 

          })

          var subjects = []
          // now for the subject fields
          mij.fields.forEach((f)=>{


            var r = Object.assign({},r_base)


            if (f['600']){
              if (f['600'].subfields){
                r.type = "person"
                f['600'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
                  if(sf.b){
                    r.numeration = sf.b
                  }                  
                  if(sf.c){
                    r.sub_label = sf.c
                  }
                  if(sf.d){
                    r.dates = sf.d
                  }
                  if(sf.f){
                    r.date_of_work = sf.f
                  }      
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                 
                })
                subjects.push(r)
              }
            } 


            if (f['610']){
              if (f['610'].subfields){
                r.type = "coporate"
                f['610'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
          
                  if(sf.b){
                    r.sub_label = sf.b
                  }
                  if(sf.d){
                    r.dates = sf.d
                  }
                  if(sf.f){
                    r.date_of_work = sf.f
                  }
                  if(sf.c){
                    r.location = sf.c
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 


            if (f['611']){
              if (f['611'].subfields){
                r.type = "event"
                f['611'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.e){
                    r.sub_label = sf.e
                  }
                  if(sf.d){
                    r.dates = sf.d
                  }
                  if(sf.f){
                    r.date_of_work = sf.f
                  }
                  if(sf.c){
                    r.location = sf.c
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 


            if (f['630']){
              if (f['630'].subfields){
                r.type = "title"
                f['630'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }

                  if(sf.d){
                    r.dates = sf.d
                  }
                  if(sf.f){
                    r.date_of_work = sf.f
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 


            if (f['647']){
              if (f['647'].subfields){
                r.type = "event"
                f['647'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
             
                  if(sf.d){
                    r.dates = sf.d
                  }
                  if(sf.c){
                    r.location = sf.c
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 


            if (f['648']){
              if (f['648'].subfields){
                r.type = "chronological"
                f['648'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 



            if (f['650']){
              if (f['650'].subfields){
                r.type = "topic"
                f['650'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.b){
                    r.sub_label = sf.b
                  }
                  if(sf.d){
                    r.dates = sf.d
                  }

                  if(sf.c){
                    r.location = sf.c
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            }             



            if (f['651']){
              if (f['651'].subfields){
                r.type = "place"
                f['651'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 



            if (f['653']){
              if (f['653'].subfields){
                r.type = "uncontrolled"
                f['653'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                })
                subjects.push(r)
              }
            } 

            if (f['654']){
              if (f['654'].subfields){
                r.type = "topic"
                f['654'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.b){
                    r.sub_label = sf.b
                  }
                 
                  if(sf.v){
                    r.form = sf.v
                  }
                  
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 

            if (f['655']){
              if (f['655'].subfields){
                r.type = "genre"
                f['655'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.b){
                    r.sub_label = sf.b
                  }
                 
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 
            if (f['656']){
              if (f['656'].subfields){
                r.type = "occupation"
                f['656'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
               
                  if(sf.k){
                    r.sub_label = sf.k
                  }


                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 
            if (f['657']){
              if (f['657'].subfields){
                r.type = "function"
                f['657'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
                  if(sf.v){
                    r.form = sf.v
                  }
                  if(sf.x){
                    r.general = sf.x
                  }
                  if(sf.y){
                    r.chronological = sf.y
                  }
                  if(sf.z){
                    r.geographic = sf.z
                  }                   
                })
                subjects.push(r)
              }
            } 
            if (f['658']){
              if (f['658'].subfields){
                r.type = "curriculum"
                f['658'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }               
                  if(sf.b){
                    r.sub_label = sf.b
                  }                                   
                })
                subjects.push(r)
              }
            } 
            if (f['662']){
              if (f['662'].subfields){
                r.type = "place"
                f['662'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.label = sf.a
                  }
                  if(sf.c){
                    r.numeration = sf.c
                  }  
               
                  if(sf.b){
                    r.sub_label = sf.b
                  }
                  if(sf.d){
                    r.location = sf.d
                  }                 
                })
                subjects.push(r)
              }
            }                                     





          })


          // console.log(subjects)
          process.stdout.write('Progress: '+count+'\r');
          return subjects
        }) 
        .map(H.curry(insertIntoDB))
        .nfcall([])
        .parallel(5)
        .done(()=>{
          callback(null,file)
        })

    }






    H(files)
      .map((f) =>{

        return f

      })
      .compact()
      .map(H.curry(processMarc))
      .nfcall([])
      .parallel(1)
      .done(()=>{

        // fs.writeFileSync('all_music_lcc_count.json',JSON.stringify(lccCount,null,2))
        db.close();

      })

})