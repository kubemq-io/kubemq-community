import{a as p,o,c as s,w as m,m as n,r,i as c,h as l,q as _,j as C,u as f,s as b,F as v,l as x}from"./entry.86b9e19f.js";import g from"./StatStripCardBox.818f187b.js";const h={class:"card container"},k={class:"item"},w={class:"flex align-items-center"},y=p({__name:"StatStripCard",props:{items:{type:Object,default:()=>[]},borderColor:{type:String,default:""},backgroundColor:{type:String,default:""}},setup(t){const a=t;return(S,B)=>{const i=r("Divider"),d=r("Card");return o(),s(d,{class:n(["shadow-none border-round-xl border-1",a.borderColor])},{content:m(()=>[c("div",h,[(o(!0),l(v,null,_(t.items,(e,u)=>(o(),l("div",k,[c("div",w,[C(f(g),{caption:e.caption,value:e.value,icon:e.icon,subCaption:e.subCaption,class:"flex-grow-1"},null,8,["caption","value","icon","subCaption"]),u!==t.items.length-1?(o(),s(i,{key:0,class:n(["flex-grow-0",a.backgroundColor]),layout:"vertical",style:{width:"1px"}},null,8,["class"])):b("",!0)])]))),256))])]),_:1},8,["class"])}}}),j=x(y,[["__scopeId","data-v-b87ef05f"]]);export{j as default};