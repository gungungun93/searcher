package searcher

import (
	"gopkg.in/mgo.v2/bson"
)

type M map[string]interface{}

type (
    Blog struct{
      Blog_id               bson.ObjectId `bson:"blog_id,omitempty"`
      Title                 string
      Content               string
      Tags                  []string
    }

	Inverted_Index struct {
		Term				string			`json:"term,omitempty"`
		Blog_id				bson.ObjectId	`bson:"blog_id,omitempty"`
		Tf					float64			`json:"tf"`
		Tf_Idf				float64			`json:"tf_idf"`
	}

	Term_Weight struct {
		Term				string			`json:"term,omitempty"`
		Total_blogs			int 			`json:"total_blogs"`
		Idf					float64			`json:"idf"`
	}
)