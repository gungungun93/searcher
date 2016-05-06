package searcher

import(
	"math"
	"sort"
	"gopkg.in/mgo.v2/bson"
	"fmt"
)

//-----------------------------------------------------------------------------
// Similarity Structure
//-----------------------------------------------------------------------------
// Contains an ID to a certain blog and its cosine similarity value, which
// depends on each query, to determine relevancy of the document.
//-----------------------------------------------------------------------------
type Similarity struct {
	blog bson.ObjectId
	cosine float64
}
// Similarities is a type of an array (slice) of "Similarity" structure.
// It is used as an interface for build-in sort package to sort a list of
// document IDs based on its relevancy with the query.
type Similarities []Similarity
//-----------------------------------------------------------------------------
// Similarities Interface Functions
//-----------------------------------------------------------------------------
// Functions used by sort.Sort method for "Similarities"
//-----------------------------------------------------------------------------
func (slice Similarities) Len() int {
	return len(slice)
}

func (slice Similarities) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (slice Similarities) Less(i, j int) bool {
	return slice[i].cosine < slice[j].cosine
}
//-----------------------------------------------------------------------------
// Retrieving Functions
//-----------------------------------------------------------------------------
// 1. Query:
// I   - Tokenizes distinct terms from query text.
// II  - Fetch all blogs containing terms which are part of the query.
// III - Perform ranking to order by most relevant results.
func (data *DBConnector) Query(keyword string) []bson.ObjectId {
	// 1. Tokenize search text into keywords
	data.tokenizer.SetText(keyword)
	termFound := make(map[string]bool)
	terms := make([]string, 0)

	// 2. Read each token
	for data.tokenizer.HasNext() {
		// 3. Get next token
		token := data.tokenizer.Next()
		// 4. Ignore whitespace tokens or terms which have already been found
		if _, found := termFound[token.GetText()]; found {
			continue
		} else if token.IsSpace() {
			continue
		// 5. Check if the term is found for the first time
		} else {
			// 6. Mark the term as found
			termFound[token.GetText()] = true
			// 7. Add the term to list
			terms = append(terms, token.GetText())
		}
	}
	fmt.Println(terms)
	// 8. Fetch all blogs containing any words from the query text
	var blogs []bson.ObjectId = data.retrieve(terms)
	fmt.Println(blogs)
	// 9. Rank all blogs and sort them according to similarity
	blogs = data.rank(terms, blogs)
	// 10. Return results
	return blogs
}
//-----------------------------------------------------------------------------
// 2. retrieve: Internal use
// Fetches all distinct blogs (Only IDs to the blogs) containing terms which
// is also contained in query terms.
func (data *DBConnector) retrieve(terms []string) []bson.ObjectId {
	// 1. Create a list to contain all "Blog ID"

	var results []bson.ObjectId = []bson.ObjectId{}
	// 2. Fetch all distinct "Blog ID" from "Inverted Index" containing the list of terms
	data.termTable.Find(bson.M{"term" : bson.M{"$in" : terms}}).Distinct("blog_id", &results)
	// 3. Return Results
	return results
}
//-----------------------------------------------------------------------------
// Ranking Functions
//-----------------------------------------------------------------------------
// 1. rank: Internal use
// I   - Create a map of each query term to its Inverse Document Frequency Value
// II  - Create a map of each term for each blog to its Tf_Idf Value.
// III - Find cosine similarity and record it to each blog.
// IV  - Repeat II until all blogs have cosine similarity value.
// V   - Sort the blogs according to its cosine similarity value in descending order.
// VI  - Return only a list of BlogIDs in the same order sorted.
func (data *DBConnector) rank(terms []string, blogs []bson.ObjectId) []bson.ObjectId {
	// 1. Retireve Inverse Document Frequency Value for each query term
	query := data.queryRank(terms)
	blogTerms := []Inverted_Index{}
	results := []Similarity{}

	// 2. Read in each search results
	for _, i := range blogs {
		// 3. Retreive all terms in the blog which is also in the query
		data.termTable.Find(bson.M{"$in" : bson.M{"term" : terms}, "blog_id" : i}).All(&blogTerms)
		// 4. Match each terms retrieved with its Tf_Idf
		blog := arrangeTerms(blogTerms)
		// 5. Find the cosine similarity between query and the blog
		results = append(results, Similarity{blog : i, cosine : cosineSimilarity(query, blog)})
	}

	// 6. Sort the list of blog Ids according to the similarity value
	sort.Sort(Similarities(results))

	// 7. Return only the list of blog IDs
	blogIDs := []bson.ObjectId{}
	for _, i := range results {
		blogIDs = append(blogIDs, i.blog)
	}

	// 8. Return results
	return blogIDs
}
//-----------------------------------------------------------------------------
// 2. queryRank: Internal use
// Maps distinct terms in query to its corresponding Inverse Document
// Frequency Value.
func (data *DBConnector) queryRank(terms []string) map[string]float64 {
	// 1. Retireve Inverse Document Frequency Value for each query term
	results := make(map[string]float64)
	var idf float64
	// 2. Read each term
	for _, i := range terms {
		// 3. Assign IDF value according to its corresponding term
		data.weightTable.Find(bson.M{"term" : i}).Select(bson.M{"idf" : 1}).One(&idf)
		results[i] = idf
	}
	// 3. Return results
	return results
}
//-----------------------------------------------------------------------------
// 3. arrangeTerms: Internal use
// Maps given distinct terms in a blog which are also in the query with its
// corresponding Tf_Idf Value.
func arrangeTerms(terms []Inverted_Index) map[string]float64 {
	// 1. Create a map of each term to its Tf_Idf value
	results := make(map[string]float64)

	// 2. Read each term retrieved from the blog
	for _, i := range terms {
		// 3. Assign its Tf_Idf value to the term
		results[i.Term] = i.Tf_Idf
	}

	// 4. Return results
	return results
}
//-----------------------------------------------------------------------------
// 4. cosineSimilarity: Internal use
// Calculates Cosine Similarity Value of given blog with the query.
func cosineSimilarity(query map[string]float64, blog map[string]float64) float64 {
	return crossProduct(query, blog) / (magnitude(query) * magnitude(blog))
}
//-----------------------------------------------------------------------------
// 5. crossProduct: Internal use
// Part of finding Cosine Similarity Value. Calculates a cross product given
// blog with the query.
func crossProduct(query map[string]float64, blog map[string]float64) float64 {
	// 1. Initialize results
	results := 0.0

	// 2. Read each distinct term in blog, which is also in query
	for key, value := range blog {
		// 3. Accumulate results of a product between query's IDF and blog's Tf_Idf
		results += (value * query[key])
	}

	// 4. Return results
	return results
}
//-----------------------------------------------------------------------------
// 6. magnitude: Internal use
// Part of finding Cosine Similarity Value. Calculates a magnitude of any
// given vectors.
func magnitude(vector map[string]float64) float64 {
	// 1. Initialize results
	results := 0.0

	// 2. Read each values in the map
	for _, value := range vector {
		// 3. Accumulate results of a square of each value
		results += (value * value)
	}

	// 4. Return the result as a square root form
	return math.Sqrt(results)
}