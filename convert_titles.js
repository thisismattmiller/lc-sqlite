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


var stmt = db.prepare(`DROP TABLE IF EXISTS titles`).run()
var stmt = db.prepare(`CREATE TABLE titles(
          lccn TEXT PRIMARY KEY,
          isbn TEXT,
          issn TEXT,
          type TEXT,
          date NUMBER,
          language TEXT,
          language_orginal TEXT,
          lcc_class TEXT,
          lcc_item TEXT,
          title TEXT,
          subtitle TEXT,
          title_sor TEXT,
          publication_place TEXT,
          publication_name TEXT,
          publication_date TEXT,
          creator TEXT,
          creator_date_start NUMBER,
          creator_date_end NUMBER,
          physical_extent TEXT,
          physical_details TEXT,
          physical_dimensions TEXT,
          contributors TEXT,
          notes TEXT,
          subjects TEXT,
          links TEXT
        );`).run()

 
glob(path, {}, function (er, files) {

    var processMarc = function (file, callback) {

      var insertIntoDB = (data,cb) => {

        data.contributors = data.contributors.join(' | ')
        data.subjects = data.subjects.join(' | ')
        data.links = data.links.join(' | ')


        var stmt = db.prepare('INSERT INTO titles (lccn, isbn, issn, type, date, language, language_orginal, lcc_class, lcc_item, title, subtitle, title_sor, publication_place, publication_name, publication_date, creator, creator_date_start, creator_date_end, physical_extent, physical_details, physical_dimensions, contributors, notes, subjects, links) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
        stmt.run(data.lccn, data.isbn, data.issn, data.type, data.date, data.language, data.language_orginal, data.lcc_class, data.lcc_item, data.title, data.subtitle, data.title_sor, data.publication_place, data.publication_name, data.publication_date, data.creator, data.creator_date_start, data.creator_date_end, data.physical_extent, data.physical_details, data.physical_dimensions, data.contributors, data.notes, data.subjects, data.links)
        cb(null,"yeah")

      }
      var materialType = null
      if (file.search('BooksAll.2014')>-1) materialType = "book"
      if (file.search('Maps.2014')>-1) materialType = "map"
      if (file.search('Serials.2014')>-1) materialType = "serial"
      if (file.search('Visual.Materials.2014')>-1) materialType = "visual"  




      H(new marc.Iso2709Reader(fs.createReadStream(file)))
        .map((record) => {
          count++
          
          var mij = record.toMiJ()

          var r = {
            lccn: null, // removes spaces and '//XX notations'
            isbn: null, // removes comments like (pbk)
            issn: null, // removes comments like (pbk)
            type: materialType,
            date: null, // number
            language: null,
            language_orginal: null,
            lcc_class: null,
            lcc_item: null,
            title: null,
            subtitle: null,
            title_sor: null,
            publication_place: null,
            publication_name: null,
            publication_date: null, 
            creator: null,
            creator_date_start: null, // number
            creator_date_end: null, // number
            physical_extent: null,
            physical_details: null,
            physical_dimensions: null,
            contributors: [],
            notes: '',
            subjects: [],
            links: [],

            // cataloging_source: null 
          }

          mij.fields.forEach((f)=>{

            if (f['010']){
              if (f['010'].subfields){              
                f['010'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.lccn = sf.a.split('//')[0].replace(/\s/g,'')
                  }
                })
              }
            }
            if (f['020']){
              if (f['020'].subfields){
                f['020'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.isbn = sf.a.replace(/\(/,' (').split(" ")[0].replace(/\:/g,'').split(" ")[0]
                    // if it looks weird then let it be weird
                    if (isNaN(r.isbn.replace(/x/gi,''))){
                     r.isbn = sf.a.trim()
                    }                    
                  }
                })
              }
            }
            if (f['022']){
                f['022'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.issn = sf.a.replace(/\(/,' (').split(" ")[0].replace(/\:/g,'').split(" ")[0]
                  }
                })
            }
            // if (f['040']){
            //     f['040'].subfields.forEach((sf)=>{
            //       if(sf.a){
            //         r.cataloging_source = sf.a
            //       }
            //     })
            // }            
            if (f['041']){
              if (f['041'].subfields){
                f['041'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.language = sf.a
                  }
                  if(sf.h){
                    r.language_orginal = sf.h
                  }                  
                })
              }
            }
            if (f['050']){
                if (f['050'].subfields){
                  f['050'].subfields.forEach((sf)=>{
                    if(sf.a){
                      r.lcc_class = sf.a
                    }
                    if(sf.b){
                      r.lcc_item = sf.b
                    }                  
                  })
                }
            }
            if (f['082']){
              if (f['082'].subfields){
                f['082'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.dewey = sf.a
                  }
                })
              }
            } 

            // if (f['006']){
            //     console.log(f['006'])
            //     // f['082'].subfields.forEach((sf)=>{
            //     //   if(sf.a){
            //     //     r.dewey = sf.a
            //     //   }
            //     // })
            // } 
            if (f['008']){
                if (!isNaN(f['008'].substring(7,11))){
                  r.date = parseInt(f['008'].substring(7,11))
                }
            } 
            if (f['245']){
              if (f['245'].subfields){
                f['245'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.title = sf.a
                  }
                  if(sf.b){
                    r.subtitle = sf.b
                  }                  
                  if(sf.c){
                    r.title_sor = sf.c
                  }
                })
              }
            } 
            if (f['260']){
              if (f['260'].subfields){
                f['260'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.publication_place = sf.a
                  }
                  if(sf.b){
                    r.publication_name = sf.b
                  }                  
                  if(sf.c){
                    r.publication_date = sf.c
                  }
                })
              }
            }             

            if (f['100']){
                var a = ""
                var b = ""
                var c = ""
                var d = ""
                if (f['100'].subfields){
                  f['100'].subfields.forEach((sf)=>{
                    if(sf.a) a = sf.a
                    if(sf.b) b = sf.b
                    if(sf.c) c = sf.c
                    if(sf.d) d = sf.d
                  })
                  r.creator = `${a} ${b} ${c}`.replace(/\s+/g,' ')
                  var dates = d.match(/[0-9]+/g)
                  if (dates && dates[0]) r.creator_date_start = dates[0]
                  if (dates && dates[1]) r.creator_date_end = dates[1]
                }
            } 

            if (f['111']){
                var a = ""
                var n = ""
                var c = ""
                var d = ""  
                if (f['111'].subfields){
                  f['111'].subfields.forEach((sf)=>{
                    if(sf.a) a = sf.a
                    if(sf.n) n = sf.n
                    if(sf.c) c = sf.c
                    if(sf.d) d = sf.d
                  })
                  r.creator = `${a} ${n} ${d} ${c}`.replace(/\s+/g,' ')
                  var dates = d.match(/[0-9]{4}/g)
                  if (dates && dates[0]) r.creator_date_start = dates[0]
                }
            } 
            if (f['110']){
              var a = ""
              var b = ""
              var c = ""
              var d = ""
              if (f['110'].subfields){              
                f['110'].subfields.forEach((sf)=>{
                  if(sf.a) a = sf.a
                  if(sf.b) b = sf.b
                  if(sf.c) c = sf.c
                  if(sf.d) d = sf.d
                })
                r.creator = `${a} ${b} ${d} ${c}`.replace(/\s+/g,' ')
                var dates = d.match(/[0-9]{4}/g)
                if (dates && dates[0]) r.creator_date_start = dates[0]
              }
            } 
            if (f['130']){
              if (f['130'].subfields){ 
                f['130'].subfields.forEach((sf)=>{
                  if(sf.a){
                    r.creator = sf.a
                  }
                })
              }
            } 
            if (f['300']){
              if (f['300'].subfields){ 
                f['300'].subfields.forEach((sf)=>{
                  if(sf.a) r.physical_extent = sf.a
                  if(sf.b) r.physical_details = sf.b
                  if(sf.c) r.physical_dimensions = sf.c
                })
              }
            } 

            if (f['700']){
              var a = ""
              var b = ""
              var c = ""
              var d = ""      
              if (f['700'].subfields){         
                f['700'].subfields.forEach((sf)=>{
                  if(sf.a) a = sf.a
                  if(sf.b) b = sf.b
                  if(sf.c) c = sf.c
                  if(sf.d) d = sf.d
                })
                var name = `${a} ${b} ${c} ${d}`.replace(/\s+/g,' ')
                r.contributors.push(name)
              }
            } 
            if (f['710']){
              var a = ""
              var b = ""
              var c = ""
              var d = ""              
              if (f['710'].subfields){ 
                f['710'].subfields.forEach((sf)=>{
                  if(sf.a) a = sf.a
                  if(sf.b) b = sf.b
                  if(sf.c) c = sf.c
                  if(sf.d) d = sf.d
                })
                var name = `${a} ${b} ${c} ${d}`.replace(/\s+/g,' ')
                // console.log(f['710'])
                // console.log(name)
                r.contributors.push(name)
              }
            } 
            if (f['711']){
              var a = ""
              var n = ""
              var c = ""
              var d = ""
              if (f['711'].subfields){               
                f['711'].subfields.forEach((sf)=>{
                  if(sf.a) a = sf.a
                  if(sf.n) n = sf.n
                  if(sf.c) c = sf.c
                  if(sf.d) d = sf.d
                })
                var name = `${a} ${n} ${d} ${c}`.replace(/\s+/g,' ')
                // console.log(f['711'])
                // console.log(name)
                r.contributors.push(name)
              }
            } 
            if (f['720']){
              f['720'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['730']){
              f['730'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['740']){
              f['740'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['751']){
              f['751'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['752']){
              f['752'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['752']){
              f['752'].subfields.forEach((sf)=>{
                if(sf.a) r.contributors.push(sf.a)
              })              
            } 
            if (f['856']){
              var link = ''
              f['856'].subfields.forEach((sf)=>{
                if(sf['3']) link = sf['3'] + '-'
                if(sf.u) link = link + sf.u
              }) 
              r.links.push(link)
            } 
            Object.keys(f).forEach((k)=>{

              if (k.match(/5[0-9]{2}/)){
                if (f[k].subfields){
                  f[k].subfields.forEach((sf)=>{
                    Object.keys(sf).forEach((kk)=>{
                      r.notes = r.notes + sf[kk] + '\n'
                    })
                  })  
                }
              }

              if (k.match(/6[0-9]{2}/)){
                var subject = ""
                if (f[k].subfields){
                  f[k].subfields.forEach((sf)=>{                  
                    Object.keys(sf).forEach((kk)=>{
                      if (kk === '$2') return
                      subject = subject + sf[kk] + '--'
                    })
                  })
                }
                subject = subject.substring(0,subject.length-2)
                r.subjects.push(subject)
              }
            })



          })
          // console.log(r)
          process.stdout.write('Progress: '+count+'\r');
          return r
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

