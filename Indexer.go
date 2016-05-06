package searcher

import(
	"tokenizer"
	"math"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"fmt"
	"log"
)

const(
	LOCALHOST = "localhost"
	DATABASE_NAME = "bugcoli"
	TERM_TABLE_NAME = "inverted_index"
	WEIGHT_TABLE_NAME = "term_weight"
	BLOG_TABLE_NAME = "post"
)

//-----------------------------------------------------------------------------
// DBConnector Structure
//-----------------------------------------------------------------------------
// Contains connectors to 3 essential database collections in MongoDB
// and a Thai language tokenizer
//-----------------------------------------------------------------------------
type DBConnector struct {
	termTable *mgo.Collection
	weightTable *mgo.Collection
	blogTable *mgo.Collection
	tokenizer *tokenizer.LongLexto
}
//-----------------------------------------------------------------------------
// DBConnector Private Mathematic Functions
//-----------------------------------------------------------------------------
// 1. euclidean_norm: Mathematical function
// Calculates Euclidean Normal Value - Square Rooot of sum of occurences of each term squared
func euclidean_norm(frequency map[tokenizer.Token]int) float64 {
	result := 0.0

	// 1. Gain access to each occurence
	for _, value := range frequency {
		// 2. Sum up sqaure power of each value
		result += float64(value * value)
	}

	// 3. Return results
	return math.Sqrt(result)
}
//-----------------------------------------------------------------------------
// 2. term_freq: Mathematical function
// Calculates Term Frequency Value - Occurence of term divided by normal value
func term_freq(occurences int, normalization float64) float64 {
	return float64(occurences) / normalization
}
//-----------------------------------------------------------------------------
// 3. inverse_document_freq: Mathematical function
// Calculates Inverse Document Frequency - Logarithm of total documents in the
// collection, divided by total documents containing the term
func inverse_document_freq(termBlogs int, totalBlogs int) float64 {
	return math.Log(float64(totalBlogs) / float64(termBlogs))
}
//-----------------------------------------------------------------------------
// 4. tf_idf: Mathematical function
// Calculates Term Frequency Value multiply by Inverse Document Frequency Value
func tf_idf(tf float64, idf float64) float64 {
	return tf * idf
}
//-----------------------------------------------------------------------------
// DBConnector Private Methods
//-----------------------------------------------------------------------------
// 1. count_occurences: Internal use
// Tokenizes the text in the blog content and counts occurences of each token
func (data *DBConnector) count_occurences(content string) map[tokenizer.Token]int {
	// 1. Create a map of a token string to its occurences
	frequency := make(map[tokenizer.Token]int)
	// 2. Split text into tokens
	data.tokenizer.SetText(content)

	// 3. Count occurences of each term
	for data.tokenizer.HasNext() {
		// 3.1 Get the next term
		token := data.tokenizer.Next()
		// 3.2 Ignore spaces
		if token.IsSpace() || token.IsHTML() {
			continue
		// 3.3 If term exists before, increase the counting by 1
		} else if _, found := frequency[token]; found {
			frequency[token] += 1
		// 3.4 If the term does not exist, then it is found
		// for the first time. The term will be new key
		} else {
			frequency[token] = 1
		}
	}

	// 4. Return results
	return frequency
}
//-----------------------------------------------------------------------------
// 2. updateIDF: Internal use
// Calculates the weight of each term for further searching mechanisms.
// Either register new terms or update existing terms to Term Weight Collection.
func (data *DBConnector) updateIDF(frequency map[tokenizer.Token]int) {
	// 1. Count all blogs in the database

	// fmt.Println(data.blogTable)

	cc := data.blogTable.Find(nil)
	totalBlogs, _ := cc.Count()

	// 2. Read each distinct tokens from blog contents
	weightRow := Term_Weight{}
	for key, _ := range frequency {
		// 3. Check if the term exists
		_ = data.weightTable.Find(bson.M{"term": key.GetText()}).One(&weightRow)
		// 4. If the term does not exist, add the term as new entry
		if (weightRow.Term == "") {
			data.weightTable.Insert(
				&Term_Weight{
					Term : key.GetText(),
					Idf : inverse_document_freq(1, totalBlogs),
					Total_blogs : 1,
					})
		// 5. If the term exists, update term values
		} else {
			termBlogs := weightRow.Total_blogs + 1
			data.weightTable.Update(
				bson.M{"term" : key.GetText()},
				bson.M{
					"set": bson.M{
						"idf" : inverse_document_freq(termBlogs, totalBlogs),
						"total_blogs" : termBlogs,
						},
					})
		}
	}
}
//-----------------------------------------------------------------------------
// 6. newIndexes: Internal use
// Add terms as inverted indexes to Inverted Index Collection.
// Records documents containing terms along with its Tf_Idf value
func (data *DBConnector) newIndexes(frequency map[tokenizer.Token]int, blogID bson.ObjectId, norm float64) {
	idf := Term_Weight{}

	// 1. Read each token
	for key, value := range frequency {
		// 2. Retrieve IDF value of the term
		_ = data.weightTable.Find(bson.M{"term" : key.GetText()}).One(&idf)
		// 3. Calculate the TF value of the term
		tf := term_freq(value, norm)
		// 4. Add all data as new inverted index entry
		data.termTable.Insert(
			&Inverted_Index{
				Term : key.GetText(),
				Blog_id : blogID,
				Tf : tf,
				Tf_Idf : tf_idf(tf, idf.Idf),
				})
	}
}
//-----------------------------------------------------------------------------
// DBConnector Methods
//-----------------------------------------------------------------------------
// 1. AddIndexes:
// I   - Assemble document contents, titles, and tags.
// II  - Tokenize the content and count each occurence.
// III - Find Euclidean Normal based on term occurence.
// IV  - Update Inverted Document Frequency Value of each term.
// V   - Add each term as new entry in Inverted Index Collection.
func (data *DBConnector) AddIndexes(blog Blog) {
	// 1. Assemble blog contents for tokenizing
	content := blog.Title + " " + blog.Content
	for _, i := range blog.Tags {
		content += " " + i
	}
	// 2. Count all occurences of each term
	frequency := data.count_occurences(content)
	// 3. Calculate Euclidean Normal Value for computing Term Frequency later
	norm := euclidean_norm(frequency)
	// 4. Update each term's weight in Term Weight Table
	data.updateIDF(frequency)
	// 5. Add each term in the blog to Inverted Index Table
	data.newIndexes(frequency, blog.Blog_id, norm)
}
//-----------------------------------------------------------------------------
// 2. Remove Indexes is accidently implemented due to miscommunication.
// The decision on the feature will be decided later.
func (data *DBConnector) RemoveIndexes(blog Blog) {
	weightRow := Term_Weight{}

	for data.tokenizer.HasNext() {
		token := data.tokenizer.Next()
		data.weightTable.Find(bson.M{"term": token}).One(&weightRow)
		if weightRow.Total_blogs <= 1 {
			data.weightTable.RemoveAll(bson.M{"term" : token})
		}
	}

	data.termTable.RemoveAll(bson.M{"blog_id" : blog.Blog_id})
}
//-----------------------------------------------------------------------------
// Constructor function
//-----------------------------------------------------------------------------
// 1. connect: Intended only for "Setup" function
// Attempts to connect to the database and establish link with collections
// required for performing search.
func connect() []*mgo.Collection {
	session, _ := mgo.Dial(LOCALHOST)

	defer session.Close()
	session.SetMode(mgo.Monotonic, true)

	database := session.DB(DATABASE_NAME)
	termTable := database.C(TERM_TABLE_NAME)
	weightTable := database.C(WEIGHT_TABLE_NAME)
	blogTable := database.C(BLOG_TABLE_NAME)

	return []*mgo.Collection{termTable, weightTable, blogTable}
}
//-----------------------------------------------------------------------------
// 2. Setup:
// Initiates the DBConnector struct for both indexing and searching.
func Setup(dictionary string) *DBConnector {
	var tables []*mgo.Collection = connect()
	// fmt.Println(tables)
	return &DBConnector{tables[0], tables[1], tables[2], tokenizer.Initialize(dictionary)}
}


func Setup_db_session( dictionary string, invertedDB *mgo.Collection,term_weightDB *mgo.Collection,blogDB *mgo.Collection) *DBConnector{
	return &DBConnector{invertedDB, term_weightDB, blogDB, tokenizer.Initialize(dictionary)}
	// blogs := []Blog{}
	// err := blogDB.Find(nil).All(&blogs)
	// check(err)

	// fmt.Println(blogs)

}
func (data *DBConnector) Create_index_forAllBlogs() {
	blogs := []Blog{}
	data.blogTable.Find(nil).All(&blogs)
	for _, blog := range blogs {
		data.AddIndexes(blog)
	}
}
// func (data *DBConnector) Test2() {



// 	blogs := []Blog{}
// 	err := blogDB.Find(nil).All(&blogs)
// 	check(err)
// 	// fmt.Println("test2")
// 	 fmt.Println(blogs)
// 	// fmt.Println("test3")
// 	// for _, i := range blogs {
// 	// 	data.AddIndexes(i)
// 	// }
// }
func check(err error) {
    if err != nil {
        log.Println(err)
    }
}